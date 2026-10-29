variable "tenancy_ocid" { type = string }
variable "user_ocid" { type = string }
variable "fingerprint" { type = string }
variable "private_key_path" { type = string }
variable "region" { type = string }

variable "compartment_ocid" {
  type        = string
  default     = null
  description = "Defaults to the tenancy (root compartment)."
}

variable "availability_domain_index" {
  type        = number
  default     = 0
  description = "Try 1 or 2 if Oracle reports 'Out of host capacity' for A1 shapes."
}

variable "shape" {
  type    = string
  default = "VM.Standard.A1.Flex"
}

variable "ocpus" {
  type    = number
  default = 4
}

variable "memory_gbs" {
  type    = number
  default = 24
}

variable "boot_volume_gbs" {
  type    = number
  default = 100
}

variable "ssh_allowed_cidr" {
  type        = string
  default     = "0.0.0.0/0"
  description = "GitHub-hosted runners use many IPs, so CI deploys need SSH open; access is key-only."
}

variable "admin_ssh_public_key_path" { type = string }
variable "deploy_ssh_public_key_path" { type = string }

variable "repo_url" { type = string }

variable "smtp_smarthost" { type = string }
variable "smtp_from" { type = string }
variable "smtp_username" { type = string }
variable "alert_email_to" { type = string }

variable "smtp_password" {
  type      = string
  sensitive = true
}
