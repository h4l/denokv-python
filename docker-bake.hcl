group "default" {
  targets = ["denokv"]
}

// Build the denokv image with the local platform. The published image is only
// amd64.
target "denokv" {
  context = "https://github.com/denoland/denokv.git"
  tags = ["denokv"]
}
