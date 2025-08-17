# Glossary

* TELEAMB: Teledetección Ambiental
* TAC: Terra-Aqua Classification?
* QA_CR: Quality Assessment - Cloud and Snow Reclassification?
* LandCover_T: is equal to 'LandCover_class' bands calculated for MODIS Terra images
* LandCover_A: is equal to 'LandCover_class' bands calculated for MODIS Aqua images
* LandCover_class: is the band that contains the classification of the land cover for MODIS images. This band
is calculated in the function binary.img_snow_landcover_reclass()
* MOD: Shorthand for MODIS Terra
* MYD: Shorthand for MODIS Aqua
* DEM: Digital Elevation Model
* BNA: Banco Nacional de Aguas
* SP: Snow Persistence
* ST: Snow Tendency
* SCA: Snow Cover Area. In the context of this project, it is the % of area (basin or otherwise) covered by snow. Expected values range [0, 100].
* CCA: Cloud Cover Area. In the context of this project, it is the % of area (basin or otherwise) covered by clouds. Expected values range [0, 100].
* MCD: ????
* CCI: Cloud Cover Index. In the context of this project, indicates a pixel is covered by clouds or not. Expected values are between 0-100. Same as Cloud_TAC? (Need to confirm)
* SCI: Snow Cover Index. In the context of this project, indicates a pixel is covered by snow or not. Expected values are between 0-100. Same as Snow_TAC? (Need to confirm)
* YTD: Year To Date
* Snow_TAC: Band that indicates presence of Snow derived using MODIS Terra and Aqua images. Expected values are between 0-100. For daily images this is a binary band, where 0 indicates no snow and 100 indicates snow presence. For monthly images, it is the percentage of days where snow was detected. e.g for a month with 15 days of snow presence, the value would be 50 (100 * 15/30).
* Cloud_TAC: Band that indicates presence of clouds derived using MODIS Terra and Aqua images. Expected values are between 0-100. For daily images this is a binary band, where 0 indicates no clouds and 100 indicates cloud presence. For monthly images, it is the percentage of days where clouds were detected. e.g for a month with 15 days of cloud presence, the value would be 50 (100 * 15/30).
* NDSI: Normalized Difference Snow Index. In the context of this project, it is a band calculated for MODIS images that indicates the presence of snow in a pixel. Expected values range [-1, 1]. Values close to 1 indicate snow presence.
