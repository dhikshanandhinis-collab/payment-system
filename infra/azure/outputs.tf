output "public_ip" {
  value = azurerm_public_ip.main.ip_address
}

output "ssh_command" {
  value = "ssh -i infra/.keys/admin_ed25519 ubuntu@${azurerm_public_ip.main.ip_address}"
}

output "grafana_tunnel" {
  value = "ssh -i infra/.keys/admin_ed25519 -L 3000:localhost:3000 ubuntu@${azurerm_public_ip.main.ip_address}  # then open http://localhost:3000"
}
