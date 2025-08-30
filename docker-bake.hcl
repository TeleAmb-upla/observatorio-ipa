# RUN THIS FIRST
# export VERSION=$(sed -n "s/version = \"\(.*\)\"/\1/p" pyproject.toml)
# docker buildx bake 

group "default" {
    targets = ["default"]
}

variable "VERSION" {
    description = "Version of the image"
}
target "default" {
    context = "."
    dockerfile = "Dockerfile"
    tags = [
        "ericklinares/gee-osn-ipa:${VERSION}",
    ]
}