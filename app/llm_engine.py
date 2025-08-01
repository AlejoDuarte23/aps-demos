import viktor as vkt
import logging
import pprint
import instructor
import plotly.graph_objects as go

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from openai import OpenAI
from openai.types.chat import ParsedChatCompletion
from typing import Union
from textwrap import dedent

from app.types import MembersDict, CrossSectionInfo
from app.plots.model_viz import plot_3d_model
from app.plots.piles import plot_3d_with_foundations, collect_support_nodes, group_four_pile_sets
from app.plots.footings import plot_structure_with_footing, group_four_pedestal_sets, collect_support_nodes as collect_footnng_support_nodes
from app.geometry.utils import get_nodes_lines

logger = logging.getLogger(__name__)
load_dotenv()
client = instructor.from_openai(OpenAI())

class PlotModel(BaseModel):
    pass 

class PlotModelWithPiles(BaseModel):
    PILE_DIAM: float = Field(..., description="Pile diameter")
    PILE_LENGTH: float = Field(..., description="Pile length")
    CAP_THICK: float = Field(..., description="Pile Cap thickness")
    EDGE_COVER: float = Field(..., description="distance between the piles and the cap edge")
    CLUSTER_TOL: float = Field(..., description="Cluter Tolerance default 3m")

class PlotFootingModel(BaseModel):
    PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
    PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
    SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
    EDGE_COVER: float = Field(..., description="Distance from outer pedestals to the slab edge.")
    CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")
# FOUNDATION_PARAMS = {
#     'PILE_DIAM': 1.0,       # meters
#     'PILE_LENGTH': 10.0,    # meters
#     'CAP_THICK': 1.2,       # meters
#     'EDGE_COVER': 0.4,      # meters
#     'CLUSTER_TOL': 3.0      # meters}

class Response(BaseModel):
    response: str = Field(..., description="Be conversational firendly and Format the response always nicely")
    selected_tool: Union[None , PlotModel, PlotModelWithPiles | PlotFootingModel] = Field(..., description="Select any of these tools, PlotModel to let the user visualize the model")


def llm_response(conversation_history: list[dict],
                 verbose: bool = True) -> ParsedChatCompletion[Response]:
    
    messages = []
    # Default system prompt is always first
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a helpful assistant with the following context, who formats responses clearly and helps users analyze structures comming
            from Autodesk Construcction Cloud (ACC) using the VITKOR - APS Integration.


            **Important**: Every time `selected_tool` is not `None`, the VIKTOR app will render the model with the specified inputs. You may say something like, “The structure will be rendered on the right-hand side of the application.”
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    if verbose:
        logger.debug("Request messages:\n%s", pprint.pformat(messages))
    
    resp_chunks = client.chat.completions.create_partial(
        model="gpt-4.1",
        messages=messages,
        response_model=Response,
        temperature=0.3,
    )

    resp_final = None
    # Streaming
    for resp in resp_chunks:
        if verbose:
            logger.debug("Received response chunk:\n%s", resp)
        resp_final: Response = resp 

    return resp_final

def get_model():
    raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
    nodes, lines = get_nodes_lines(file=raw)


    my_cs = CrossSectionInfo(
        name = "L70x4",
        id = 1,
        A=10,
        Iz=100,
        Iy=1000,
        Jxx=2000,
        b=0.070,
        h=0.070
    )
    members: MembersDict = {}
    cs_dict = {1 : my_cs}
    for line_id in lines:
        members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}
    print(nodes, lines, members, cs_dict)
    return nodes, lines, members, cs_dict

def execute_tool(response: Response) -> tuple[str, go.Figure | None]:
    """Exectue the tools based on the user query and file_content. Generates a text response
    or a Plotly view."""
    print(f"[Debug] {response}")
    if isinstance(response.selected_tool, PlotModel):
        nodes, lines, members, cs_dict = get_model()
        fig = plot_3d_model(nodes, lines, members, cs_dict)
        print(fig)
        return response.response, fig
    
    if isinstance(response.selected_tool, PlotModelWithPiles):
        nodes, lines, members, cs_dict = get_model()
        foundation_params_dict = response.selected_tool.model_dump()
        support_nodes = collect_support_nodes(nodes)
        caps = group_four_pile_sets(support_nodes, cluster_tol=foundation_params_dict['CLUSTER_TOL'])

        fig = plot_3d_with_foundations(nodes=nodes, lines=lines, members=members,cross_sections=cs_dict, caps=caps, foundation_params=foundation_params_dict)
        return response.response, fig
            
    if isinstance(response.selected_tool, PlotFootingModel):
        nodes, lines, members, cs_dict = get_model()
        footing_params_dict = response.selected_tool.model_dump()
        support_nodes = collect_footnng_support_nodes(nodes)
        footings = group_four_pedestal_sets(support_nodes, cluster_tol=footing_params_dict['CLUSTER_TOL'])

        fig = plot_structure_with_footing(nodes=nodes, lines=lines, members=members, cross_sections=cs_dict, footings=footings, footing_params=footing_params_dict)
        return response.response, fig
    
    return response.response, None