# Terraform Settings Block
terraform {
  required_version = ">= 1.0.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.0" # Optional but recommended in production
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  subscription_id = "34cada73-4cb3-4353-ad22-9691a655be3c"
}

# Create Resource Group 
resource "azurerm_resource_group" "balu_demo_rg1" {
  location = "eastus"
  name     = "balu-demo-rg1"
}
