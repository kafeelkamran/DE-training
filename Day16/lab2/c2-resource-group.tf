# Resource-1: Azure Resource Group
resource "azurerm_resource_group" "k_myrg" {
  name = "k_myrg-1"
  location = "North Central US"
}