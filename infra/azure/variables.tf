variable "location" {
  type        = string
  default     = "centralindia"
  description = "Azure for Students only allows some regions; if apply fails with RequestDisallowedByAzure, use one listed in the error."
}

variable "resource_group_name" {
  type    = string
  default = "payment-system-prod"
}

variable "vm_size" {
  type        = string
  default     = "Standard_B2s"
  description = "2 vCPU / 4 GB (+ swap). Fits the student vCPU quota and keeps the monthly credit burn low."
}

variable "os_disk_gb" {
  type    = number
  default = 64
}

variable "swap_size" {
  type    = string
  default = "4G"
}

variable "auto_shutdown_time" {
  type        = string
  default     = "2300"
  description = "Daily automatic shutdown (HHmm) in auto_shutdown_timezone; empty string disables it."
}

variable "auto_shutdown_timezone" {
  type    = string
  default = "India Standard Time"
}

variable "ssh_allowed_source" {
  type        = string
  default     = "*"
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
