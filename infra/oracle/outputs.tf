output "public_ip" {
  value = oci_core_instance.app.public_ip
}

output "ssh_command" {
  value = "ssh -i infra/.keys/admin_ed25519 ubuntu@${oci_core_instance.app.public_ip}"
}

output "grafana_tunnel" {
  value = "ssh -i infra/.keys/admin_ed25519 -L 3000:localhost:3000 ubuntu@${oci_core_instance.app.public_ip}  # then open http://localhost:3000"
}
