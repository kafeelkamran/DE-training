#Terraform Setting block
terraform {
    required_version = ">= 1.0.0"
    required_providers {
        azurerm = {
            source = "hashicorp/azurerm"
            version = ">= 2.0"
        }
    }
}

#configure the microsoft azure provider
provider "azurerm"{
    features{
    }
    subscription_id = "ed28a75e-1696-47c5-a7f6-ca2ffaf2378f"
}

#create Resource group
resource "azurerm_resource_group" "k_demo_rg1"{
    location = "northcentralus"
    name = "k_demo_rg1"
}