# RUN THIS FIRST
# export VERSION=$(sed -n "s/version = \"\(.*\)\"/\1/p" pyproject.toml)
# docker buildx bake 

group "default" {
    targets = ["ipa"]
}

group "ipa" {
    targets = ["ipa"]
}

group "web" {
    targets = ["web"]
}

group "all" {
    targets = ["ipa", "web"]
}


variable "VERSION" {
    description = "Version of the image"
}
variable "WEB_VERSION" {
    description = "Version of the web image"
    default = "0.1.0"
}
target "ipa" {
    context = "."
    dockerfile = "Dockerfile"
    tags = [
        "ericklinares/gee-osn-ipa:${VERSION}",
    ]
}

target "web" {
    context = "."
    dockerfile = "Dockerfile.web"
    tags = [
        "ericklinares/gee-osn-ipa-web:${WEB_VERSION}",
    ]
}