package vmworkflow

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/datastore"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/provider"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/storagepod"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/structure"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/viapi"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/virtualdevice"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/types"
)

// SDRSTransformVirtualMachineConfigSpecForCreate performs storage DRS
// transformations to a VirtualMachineConfigSpec for the "bare metal", or from
// scratch, VM creation path. This is accomplished by doing a storage DRS
// recommendation against the datastore cluster specified, with the
// recommendations from the operation applied to the config spec directly. The
// recommendations themselves are dropped after the fact and left to expire.
func SDRSTransformVirtualMachineConfigSpecForCreate(
	d *schema.ResourceData,
	client *govmomi.Client,
	spec types.VirtualMachineConfigSpec,
	pool *object.ResourcePool,
) (types.VirtualMachineConfigSpec, error) {
	if err := viapi.ValidateVirtualCenter(client); err != nil {
		return spec, errors.New("assignment of a virtual machine to a datastore cluster requires vCenter")
	}

	log.Printf("[DEBUG] %s: Getting storage DRS recommendations for VM creation", ResourceIDString(d))

	recommendations, err := recommendDatastoresForCreate(d, client, spec, pool)
	if err != nil {
		return spec, err
	}

	spec, err = applySDRSRecommendationsToConfigSpec(d, client, recommendations, spec)
	if err != nil {
		return spec, fmt.Errorf("error applying SDRS recommendations to config spec: %s", err)
	}

	log.Printf("[DEBUG] %s: Storage DRS recommendations applied successfully", ResourceIDString(d))
	return spec, nil
}

// SDRSTransformVirtualMachineCloneSpec performs storage DRS transformations to
// a VirtualMachineCloneSpec for the cloned VM creation path. This is
// accomplished by getting recommendations for the final VM configuration, and
// then applying those recommendations to the clone spec. We do this instead of
// asking for recommendations for the clone spec itself as the final VM
// configuration could have differing disk parameters from the source virtual
// machine, such as a larger size.
func SDRSTransformVirtualMachineCloneSpec(
	d *schema.ResourceData,
	client *govmomi.Client,
	configSpec types.VirtualMachineConfigSpec,
	cloneSpec types.VirtualMachineCloneSpec,
	pool *object.ResourcePool,
	configDevices object.VirtualDeviceList,
	cloneDevices object.VirtualDeviceList,
) (types.VirtualMachineCloneSpec, error) {
	if err := viapi.ValidateVirtualCenter(client); err != nil {
		return cloneSpec, errors.New("assignment of a virtual machine to a datastore cluster requires vCenter")
	}

	log.Printf("[DEBUG] %s: Getting storage DRS recommendations for VM cloning", ResourceIDString(d))

	recommendations, err := recommendDatastoresForCreate(d, client, configSpec, pool)
	if err != nil {
		return cloneSpec, err
	}

	cloneSpec, err = applySDRSRecommendationsToCloneSpec(d, client, recommendations, cloneSpec, cloneDevices, configDevices)
	if err != nil {
		return cloneSpec, fmt.Errorf("error applying SDRS recommendations to clone spec: %s", err)
	}

	log.Printf("[DEBUG] %s: Storage DRS recommendations applied successfully", ResourceIDString(d))
	return cloneSpec, nil
}

func storagePlacementSpecForCreate(
	d structure.ResourceIDStringer,
	spec types.VirtualMachineConfigSpec,
	pool *object.ResourcePool,
	pod *object.StoragePod,
) types.StoragePlacementSpec {
	log.Printf("[DEBUG] %s: Creating StoragePodPlacementSpec for creation", ResourceIDString(d))

	pr := pool.Reference()
	sps := types.StoragePlacementSpec{
		Type:         string(types.StoragePlacementSpecPlacementTypeCreate),
		ResourcePool: &pr,
		ConfigSpec:   &spec,
	}
	sps.PodSelectionSpec = storageDrsPodSelectionSpecForCreate(d, spec, pod)

	return sps
}

func storageDrsPodSelectionSpecForCreate(
	d structure.ResourceIDStringer,
	spec types.VirtualMachineConfigSpec,
	pod *object.StoragePod,
) types.StorageDrsPodSelectionSpec {
	log.Printf("[DEBUG] %s: Creating StorageDrsPodSelectionSpec for creation", ResourceIDString(d))

	pr := pod.Reference()
	pss := types.StorageDrsPodSelectionSpec{
		StoragePod: &pr,
	}
	pss.InitialVmConfig = vmPodConfigForPlacementForCreate(d, spec, pod)

	return pss
}

func vmPodConfigForPlacementForCreate(
	d structure.ResourceIDStringer,
	spec types.VirtualMachineConfigSpec,
	pod *object.StoragePod,
) []types.VmPodConfigForPlacement {
	log.Printf("[DEBUG] %s: Creating VmPodConfigForPlacement for creation", ResourceIDString(d))
	return vmPodConfigForPlacementAppendNewDisks(nil, d, spec, pod)
}

func storagePodDiskFilter(
	deviceChange []types.BaseVirtualDeviceConfigSpec,
	operation types.VirtualDeviceConfigSpecOperation,
	fileOperation types.VirtualDeviceConfigSpecFileOperation,
) []*types.VirtualDisk {
	var disks []*types.VirtualDisk
	for _, dc := range deviceChange {
		spec := dc.GetVirtualDeviceConfigSpec()
		if spec.Operation != operation {
			continue
		}
		if spec.FileOperation != fileOperation {
			continue
		}
		d, ok := spec.Device.(*types.VirtualDisk)
		if !ok {
			continue
		}
		disks = append(disks, d)
	}
	return disks
}

func vmPodConfigForPlacementAppendNewDisks(
	configs []types.VmPodConfigForPlacement,
	d structure.ResourceIDStringer,
	spec types.VirtualMachineConfigSpec,
	pod *object.StoragePod,
) []types.VmPodConfigForPlacement {
	for _, disk := range storagePodDiskFilter(
		spec.DeviceChange,
		types.VirtualDeviceConfigSpecOperationAdd,
		types.VirtualDeviceConfigSpecFileOperationCreate,
	) {
		log.Printf(
			"[DEBUG] %s: Requesting recommendation for new disk %q on datastore cluster %q",
			ResourceIDString(d),
			object.VirtualDeviceList{}.Name(disk),
			pod.InventoryPath,
		)
		config := types.VmPodConfigForPlacement{
			StoragePod: pod.Reference(),
			Disk: []types.PodDiskLocator{
				{
					DiskId:          disk.Key,
					DiskBackingInfo: disk.Backing,
				},
			},
		}
		configs = append(configs, config)
	}
	return configs
}

func applySDRSRecommendationsToConfigSpec(
	d structure.ResourceIDStringer,
	client *govmomi.Client,
	recommendations []types.ClusterRecommendation,
	spec types.VirtualMachineConfigSpec,
) (types.VirtualMachineConfigSpec, error) {
	// Our target datastores for each individual disk reside in various locations
	// in the cluster recommendations. We use the relocate spec - we need to
	// search the relocate specs in all actions for various things.
	for _, action := range recommendations[0].Action {
		spa, ok := action.(*types.StoragePlacementAction)
		if !ok {
			continue
		}
		if len(spa.RelocateSpec.Disk) < 1 {
			// This is the recommendation for the VM configuration. Place the VMX
			// file here. This should only happen when we are creating virtual
			// machines.
			var err error
			spec, err = PopulateVmxDatastore(d, client, spec, spa.Destination.Value)
			if err != nil {
				return spec, err
			}
			continue
		}
		for _, disk := range spa.RelocateSpec.Disk {
			for _, dc := range spec.DeviceChange {
				vdcs := dc.GetVirtualDeviceConfigSpec()
				destDisk, ok := vdcs.Device.(*types.VirtualDisk)
				if !ok {
					continue
				}
				if destDisk.Key == disk.DiskId {
					// This is our disk. Populate the backing file datastore with the
					// datastore ID from this entry in the relocate spec.
					ds, err := logAndGetDatastoreForSDRSDiskAssignment(d, client, destDisk, disk.Datastore)
					if err != nil {
						return spec, err
					}
					destDisk.Backing.(*types.VirtualDiskFlatVer2BackingInfo).FileName = fmt.Sprintf("[%s]", ds.Name())
				}
			}
		}
	}
	return spec, nil
}

func applySDRSRecommendationsToCloneSpec(
	d structure.ResourceIDStringer,
	client *govmomi.Client,
	recommendations []types.ClusterRecommendation,
	cloneSpec types.VirtualMachineCloneSpec,
	sourceDevices object.VirtualDeviceList,
	specDevices object.VirtualDeviceList,
) (types.VirtualMachineCloneSpec, error) {
	// Because we are not using the clone spec as the source of truth for the
	// recommendation operation, we actually need a bunch more information than
	// we normally use for the non-clone creation operation:
	//
	// * The original config spec we used to get the recommendations
	// * The spec for the clone operation
	// * The source VM's device list
	//
	// We then have to do the following:
	//
	// * Sort the virtual disks in the configSpec and the source device list by
	// unit number.
	// * Traverse the recommendations list and:
	//  * Locate the disk for every location spec in the config spec
	//  * Check for corresponding list index in the original device list
	//  * If found, locate disk ID in clone spec, assign datastore.
	//
	// This is convoluted, but the end effect is ensuring that we have a clone
	// spec that will clone to a location that will always be able to accommodate
	// the end configuration of a VM without either failing a reconfiguration
	// operation or requiring another datastore migration to satisfy the
	// configuration, which could be costly in both time and I/O.

	// We don't need to worry about limiting the SCSI controllers we scan disks
	// for in these queries as they are irrelevant for these lists - you cannot
	// selectively choose disks to clone in a VM and the configSpec in this
	// instance will always reflect the final state of the disks anyway with all
	// of the appropriate SCSI controllers selected.
	sourceDisks := virtualdevice.SelectAndSortDisks(sourceDevices, 4)
	specDisks := virtualdevice.SelectAndSortDisks(specDevices, 4)

	for _, action := range recommendations[0].Action {
		spa, ok := action.(*types.StoragePlacementAction)
		if !ok {
			continue
		}
		if len(spa.RelocateSpec.Disk) < 1 {
			// This is the destination for the VM configuration.
			cloneSpec.Location.Datastore = &spa.Destination
			continue
		}
		for _, disk := range spa.RelocateSpec.Disk {
			// This is where we need to look for the respective disk in the device
			// list in the config spec.
			for i, specDevice := range specDisks {
				specDisk := specDevice.(*types.VirtualDisk)
				if specDisk.Key == disk.DiskId {
					// This is our disk. We now need to determine if this disk currently
					// exists in our source VM.
					if i >= len(sourceDisks) {
						continue
					}
					sourceDisk := sourceDisks[i].(*types.VirtualDisk)
					// Traverse the clone relocate spec here, and set the datastore.
					for _, destDisk := range cloneSpec.Location.Disk {
						if destDisk.DiskId == sourceDisk.Key {
							// Finally, we have the disk we need to set the location on. Set
							// the datastore.
							ds, err := logAndGetDatastoreForSDRSDiskAssignment(d, client, sourceDisk, disk.Datastore)
							if err != nil {
								return cloneSpec, err
							}
							destDisk.DiskBackingInfo.(*types.VirtualDiskFlatVer2BackingInfo).FileName = ds.Path("")
							destDisk.DiskBackingInfo.(*types.VirtualDiskFlatVer2BackingInfo).Datastore = &disk.Datastore
							destDisk.Datastore = disk.Datastore
						}
					}
				}
			}
		}
	}
	return cloneSpec, nil
}

// recommendDatastoresForCreate contains shared functionality between VM
// creation and cloning workflows for recommending datastores when when a
// datastore cluster is specified.
func recommendDatastoresForCreate(
	d *schema.ResourceData,
	client *govmomi.Client,
	spec types.VirtualMachineConfigSpec,
	pool *object.ResourcePool,
) ([]types.ClusterRecommendation, error) {
	pod, err := storagepod.FromID(client, d.Get("datastore_cluster_id").(string))
	if err != nil {
		return nil, fmt.Errorf("error locating datastore cluster for initial VM placement: %s", err)
	}
	sps := storagePlacementSpecForCreate(d, spec, pool, pod)
	srm := object.NewStorageResourceManager(client.Client)
	ctx, cancel := context.WithTimeout(context.Background(), provider.DefaultAPITimeout)
	defer cancel()
	result, err := srm.RecommendDatastores(ctx, sps)
	if err != nil {
		return nil, fmt.Errorf("error getting storage DRS recommendations: %s", err)
	}

	if len(result.Recommendations) < 1 {
		return nil, errors.New("no storage DRS recommendations were returned. Please check your datastore cluster settings and try again")
	}
	return result.Recommendations, nil
}

func logAndGetDatastoreForSDRSDiskAssignment(
	d structure.ResourceIDStringer,
	client *govmomi.Client,
	disk *types.VirtualDisk,
	ref types.ManagedObjectReference,
) (*object.Datastore, error) {
	ds, err := datastore.FromID(client, ref.Value)
	if err != nil {
		// Hopefully this never happens, but if it does we want to bubble this up
		// as there will probably be an idea applying the SDRS recommendations.
		return nil, fmt.Errorf(
			"error locating recommended datastore %q for disk %q: %s",
			ref.Value,
			object.VirtualDeviceList{}.Name(disk),
			err,
		)
	}

	log.Printf(
		"[DEBUG] %s: Assigning recommended datastore %q to disk %q",
		ResourceIDString(d),
		ds.Name(),
		object.VirtualDeviceList{}.Name(disk),
	)
	return ds, nil
}
