# Resource: Azure Linux Virtual Machine
resource "azurerm_linux_virtual_machine" "mylinuxvm" {
  name                = "balu_mylinuxvm-1"
  computer_name       = "devlinux-vm1" # Hostname of the VM
  resource_group_name = azurerm_resource_group.myrg.name
  location            = azurerm_resource_group.myrg.location
  size                = "Standard_D2s_v3"
  admin_username      = "azureuser"
  network_interface_ids = [
    azurerm_network_interface.myvmnic.id
  ]
#   admin_ssh_key {
#     username   = "azureuser"
#     public_key = file("${path.module}/ssh-keys/terraform-azure.pub")
#   }

disable_password_authentication = false
admin_password = "balu@123456"
  os_disk {
    name = "osdisk"
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }
  source_image_reference {
    publisher = "Canonical"
    offer = "0001-com-ubuntu-server-jammy"
    sku = "22_04-lts"
    version = "latest"
  }
  custom_data = filebase64("${path.module}/app-scripts/app1-cloud.txt")
}