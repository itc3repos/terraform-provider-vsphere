package vmworkflow

import (
	"fmt"
	"log"

	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/datastore"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/structure"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25/types"
)

const resourceVSphereVirtualMachineResourceName = "vsphere_virtual_machine"

// PopulateVmxDatastore populates the datastore for the virtual machine
// configuration during the creation stage of a virtual machine.
func PopulateVmxDatastore(
	d structure.ResourceIDStringer,
	client *govmomi.Client,
	spec types.VirtualMachineConfigSpec,
	id string,
) (types.VirtualMachineConfigSpec, error) {
	ds, err := datastore.FromID(client, id)
	if err != nil {
		return spec, fmt.Errorf("error locating datastore for VM configuration: %s", err)
	}

	log.Printf("[DEBUG] %s: Datastore for VMX configuration is %q", ResourceIDString(d), ds.Name())
	spec.Files = &types.VirtualMachineFileInfo{
		VmPathName: fmt.Sprintf("[%s]", ds.Name()),
	}

	return spec, nil
}

// ResourceIDString prints a friendly string for the
// vsphere_virtual_machine resource.
func ResourceIDString(d structure.ResourceIDStringer) string {
	return structure.ResourceIDString(d, resourceVSphereVirtualMachineResourceName)
}
