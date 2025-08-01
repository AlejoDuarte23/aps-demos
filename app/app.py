import base64
import urllib.parse
import viktor as vkt  # type: ignore
import app.crud.data_management as aps_helpers  
from app.views.apsView import APSView, APSresult

import os
import json
import viktor as vkt
import plotly.graph_objects as go

from openai import OpenAI
from dotenv import load_dotenv
from app.plots.model_viz import default_blank_scene
from typing import Literal
from textwrap import dedent
from app.llm_engine import llm_response, execute_tool

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def store_scene(figure: go.Figure, view_name: Literal["view"] = "view") -> None:
    """This function stores the output of a tool call in
    the vkt.Storage object. The storage object can be used to communicate
    between views."""
    vkt.Storage().set(
        view_name,
        data=vkt.File.from_data(figure.to_json().encode()),
        scope="entity",
    )

def get_aps_token():
    integration = vkt.external.OAuth2Integration("aps-integration-1")
    return integration.get_access_token()

@vkt.memoize
def get_hub_list(params, **kwargs):
    token = get_aps_token()
    hub_names = aps_helpers.get_hub_names(token)
    return hub_names if hub_names else ["No hubs found"]
@vkt.memoize
def get_projects(params, **kwargs) -> list[str]:
    if params.step1.hubs:
        token = get_aps_token()
        hub_name = params.step1.hubs
        projects = aps_helpers.get_projects_names_by_hub(token, hub_name)
        return projects
    return ["Select Hub First"]

@vkt.memoize
def get_top_folders_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        return aps_helpers.get_hub_top_folders_names(token, hub_name, project_name)
    return ["Select Project First"]

def get_subfolder_paths_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project and params.step1.top_folder:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        top_folder = params.step1.top_folder
        return aps_helpers.get_subfolders_paths_from_top_folder(token, hub_name, project_name, top_folder)
    return ["Select Top Folder First"]

def get_folder_files_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project and params.step1.subfolder_path:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        subfolder_path = params.step1.subfolder_path
        return aps_helpers.get_files_names_from_sub_folder(token, hub_name, project_name, subfolder_path)
    return ["Select Subfolder First"]

class Parametrization(vkt.Parametrization):
    step1 = vkt.Step("APS Integration", views="show_cad_model")
    step1.title = vkt.Text("# APS Integration demo")
    step1.hubs = vkt.OptionField("Avaliable Hubs", options=get_hub_list)
    step1.project = vkt.OptionField("Avaliable Projects", options=get_projects)
    step1.br2 = vkt.LineBreak()
    step1.top_folder = vkt.OptionField("Top Folders", options=get_top_folders_options)
    step1.subfolder_path = vkt.OptionField("Subfolders Path", options=get_subfolder_paths_options)
    step1.br4 = vkt.LineBreak()
    step1.files = vkt.OptionField("Folder Files", options=get_folder_files_options)
    step1.stored_file = vkt.OptionField("Select File for Agent Context", options=get_folder_files_options)
    step1.br5 = vkt.LineBreak()
    step1.agent_contenxt = vkt.ActionButton("Send File to Agent", method="store_file_in_app")
    step2 = vkt.Step("Structural Agent", views=["get_plotly_view"])
    step2.chat = vkt.Chat("", method="call_llm")
class Controller(vkt.Controller):
    parametrization = Parametrization(width=40)

    @APSView("Model Viewer", duration_guess=40)
    def show_cad_model(self, params, **kwargs):
        # 1. Get 3‑legged token from VIKTOR integration
        integration = vkt.external.OAuth2Integration("aps-integration-1")
        token = integration.get_access_token()
        cad_dict = Controller.get_cad_files_dict(params, **kwargs)
        if cad_dict:
            cad_urn = cad_dict[params.step1.files]["urn"]
            viewer_urn = Controller.to_b64url(cad_urn)
            return APSresult(urn_b64=viewer_urn, token=token)
        return None

    def get_file_content(self, params, **kwargs):
        """Get raw binary content of the selected file."""
        token = get_aps_token()
        return aps_helpers.get_file_content(
            token=token,
            hub_name=params.step1.hubs,
            project_name=params.step1.project,
            subfolder_path=params.step1.subfolder_path,
            file_name=params.step1.agent_contenxt,
        )

    @staticmethod
    def store_file_in_app(params, **kwargs):
        token = get_aps_token()
        file_content = aps_helpers.get_file_content(
            token=token,
            hub_name=params.step1.hubs,
            project_name=params.step1.project,
            subfolder_path=params.step1.subfolder_path,
            file_name=params.step1.files,
        )
        vkt.Storage().set(
            "ifc_model",
            data=vkt.File.from_data(file_content),
            scope="entity",
        )

    @staticmethod
    def get_cad_files_dict(params, **kwargs) -> None | dict[str, dict[str, str]]:
        token = get_aps_token()
        viewables = aps_helpers.get_all_cad_file_from_hub(token=token)
        return viewables

    @staticmethod
    def to_b64url(value: str) -> str:
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

    def call_llm(self, params, **kwargs) -> vkt.ChatResult | None:
            """Multi-turn conversation between the user and the agent."""
            # Get conversation
            conversation_history = params.step2.chat.get_messages()
            #  Check if user uploaded an Excel Field
            if conversation_history:
                response = llm_response(
                    conversation_history=conversation_history,
                )

                if response:
                    llm_message, fig = execute_tool(response)
                    if fig:
                        print("Storing fig")
                        store_scene(fig)
                    return vkt.ChatResult(params.step2.chat, llm_message)
                else:
                    raise ValueError("The LLM returned no parsed reponse.")
            return None

    @vkt.PlotlyView("Plotting Tool", width=100)
    def get_plotly_view(self, params, **kwargs) -> vkt.PlotlyResult:
        """This view plots the output of a tool call in a Plotly view.
        All tool calls are go.Figures exported as JSON. They are saved in
        Storage and retrieved here."""
        # 1. Delete tools calls from storage if there is no .xlsx file
        if not params.step2.chat:
            entities = vkt.Storage().list(scope="entity")
            for entity in entities:
                if entity == "view":
                    vkt.Storage().delete("view", scope="entity")

        # 2. Try to get the previous view from the tool call, otherwise blank scene
        try:
            raw = vkt.Storage().get("view", scope="entity").getvalue()
            fig = go.Figure(json.loads(raw))
        except Exception:
            fig = default_blank_scene()

        return vkt.PlotlyResult(fig.to_json())

