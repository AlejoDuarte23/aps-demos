from app.foundations.footings.footings import FootingSoilData
from typing import Literal
from instructor.dsl.partial import PartialLiteralMixin
from pydantic import BaseModel, Field

class PlotOpenSeesModelTool(BaseModel):
    plot_model: bool

class RunOpenSeesModelTool(BaseModel):
    run_model: bool

class DisplayLoadsTool(BaseModel):
    critical_load_case: str = Field(..., description="Use this tool to display the OpenSees model with critical loads from the critical load cases. The system will show it automatically.")
    pass 

class GetGeotechnicalInputsForFoundationDesign(BaseModel,PartialLiteralMixin):
    design_type: Literal["pile","footing"]

class PullGeotechnicalReportTool(BaseModel):
    file_name: str = Field(..., description="Name of the Geotechnical Report. Default: GEO001 - GEOTECHNICAL DATA SUMMARY REV0.pdf")

class FootingDesignTool(BaseModel):
    footing_soil_data: FootingSoilData  = Field(..., description="Required footing soild data for the inner system to design the optimal footing")

class UploadToAccTool(BaseModel):
    upload_to_acc: bool
    PEDESTAL_WIDTH: float = Field(..., description="Optimal Pedestal Witdth")
    PEDESTAL_HEIGHT: float = Field(..., description="Optimal Pedestal Height")
    SLAB_THICK: float = Field(..., description="Optimal Slab Thickness")
    SLAB_BASE_SIZE: float = Field(..., description="Optimal Slab Base")