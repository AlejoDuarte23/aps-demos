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

