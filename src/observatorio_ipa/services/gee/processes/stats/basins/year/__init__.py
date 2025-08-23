"""Yearly statistics for every image in a multi year Time Series ImageCollection per area of interest (basin)"""

from .sca_y_bna import SCA_Y_BNA
from .sca_y_elev_bna import SCA_Y_ELEV_BNA
from .sca_y_t_area_bna import SCA_Y_T_AREA_BNA
from .sca_y_t_elev_bna import SCA_Y_T_ELEV_BNA
from .snowline_y_bna import Snowline_Y_BNA

__all__ = [
    "SCA_Y_BNA",
    "SCA_Y_ELEV_BNA",
    "SCA_Y_T_AREA_BNA",
    "SCA_Y_T_ELEV_BNA",
    "Snowline_Y_BNA",
]
