# Usage

## Using Prefix

A prefix is required to read and save images. If no prefix is provided, the script will raise an error.

The prefix should be a string that ends with either an underscore "_" or a hyphen "-". If the provided prefix does not end with these characters, an underscore will be added to it. For example, if the provided prefix is "image", it will be changed to "image_".

## Cloud Storage

- Grant admin users "Storage Folder Admin" Role so they can manage everything within the bucket including creating folders 
- Grant object editors "Storage Object Admin" Role so they can create, edit and delete objects within the bucket
- Grant viewers "Storage Object Viewer" Role so they can view objects within the bucket