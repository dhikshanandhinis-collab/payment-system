terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

# Authenticates with the Azure CLI login (`az login`); provision.ps1 sets ARM_SUBSCRIPTION_ID.
provider "azurerm" {
  features {}
  resource_provider_registrations = "core"
}

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.tags
}

locals {
  tags = { project = "payment-system", environment = "production" }
}

resource "azurerm_virtual_network" "main" {
  name                = "payment-system-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags
}

resource "azurerm_subnet" "main" {
  name                 = "app"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_public_ip" "main" {
  name                = "payment-system-ip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags
}

# Only SSH is reachable from outside; every service port is bound to 127.0.0.1 on the VM.
resource "azurerm_network_security_group" "main" {
  name                = "payment-system-ssh-only"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  security_rule {
    name                       = "ssh"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.ssh_allowed_source
    destination_address_prefix = "*"
  }
}

resource "azurerm_network_interface" "main" {
  name                = "payment-system-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.main.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.main.id
  }
}

resource "azurerm_network_interface_security_group_association" "main" {
  network_interface_id      = azurerm_network_interface.main.id
  network_security_group_id = azurerm_network_security_group.main.id
}

resource "azurerm_linux_virtual_machine" "app" {
  name                            = "payment-system"
  location                        = azurerm_resource_group.main.location
  resource_group_name             = azurerm_resource_group.main.name
  size                            = var.vm_size
  admin_username                  = "ubuntu"
  disable_password_authentication = true
  network_interface_ids           = [azurerm_network_interface.main.id]
  tags                            = local.tags

  admin_ssh_key {
    username   = "ubuntu"
    public_key = file(var.admin_ssh_public_key_path)
  }

  admin_ssh_key {
    username   = "ubuntu"
    public_key = file(var.deploy_ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = var.os_disk_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  custom_data = base64encode(templatefile("${path.module}/../cloud-init.yaml.tftpl", {
    repo_url       = var.repo_url
    swap_size      = var.swap_size
    smtp_smarthost = var.smtp_smarthost
    smtp_from      = var.smtp_from
    smtp_username  = var.smtp_username
    smtp_password  = var.smtp_password
    alert_email_to = var.alert_email_to
  }))

  # custom_data only runs on first boot; editing it must not recreate the server (and its data).
  lifecycle {
    ignore_changes = [custom_data]
  }
}
