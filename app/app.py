import base64
import urllib.parse
import viktor as vkt  # type: ignore
import app.crud.data_management as aps_helpers  
from app.views.apsView import APSView, APSresult

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
    if params.hubs:
        token = get_aps_token()
        hub_name = params.hubs
        projects = aps_helpers.get_projects_names_by_hub(token, hub_name)
        return projects
    return ["Select Hub First"]

@vkt.memoize
def get_top_folders_options(params, **kwargs) -> list[str]:
    if params.hubs and params.project:
        token = get_aps_token()
        hub_name = params.hubs
        project_name = params.project
        return aps_helpers.get_hub_top_folders_names(token, hub_name, project_name)
    return ["Select Project First"]

def get_subfolder_paths_options(params, **kwargs) -> list[str]:
    if params.hubs and params.project and params.top_folder:
        token = get_aps_token()
        hub_name = params.hubs
        project_name = params.project
        top_folder = params.top_folder
        return aps_helpers.get_subfolders_paths_from_top_folder(token, hub_name, project_name, top_folder)
    return ["Select Top Folder First"]

def get_folder_files_options(params, **kwargs) -> list[str]:
    if params.hubs and params.project and params.subfolder_path:
        token = get_aps_token()
        hub_name = params.hubs
        project_name = params.project
        subfolder_path = params.subfolder_path
        return aps_helpers.get_files_names_from_sub_folder(token, hub_name, project_name, subfolder_path)
    return ["Select Subfolder First"]

class Parametrization(vkt.Parametrization):
    title = vkt.Text("# APS Integration demo")
    hubs = vkt.OptionField("Avaliable Hubs", options=get_hub_list)
    project = vkt.OptionField("Avaliable Projects", options=get_projects)
    br2 = vkt.LineBreak()
    top_folder = vkt.OptionField("Top Folders", options=get_top_folders_options)
    subfolder_path = vkt.OptionField("Subfolders Path", options=get_subfolder_paths_options)
    br4 = vkt.LineBreak()
    files = vkt.OptionField("Folder Files", options=get_folder_files_options)
    stored_file = vkt.OptionField("Select File for Agent Context", options=get_folder_files_options)
    br5 = vkt.LineBreak()
    agent_contenxt = vkt.ActionButton("Send File to Agent", method="store_file_in_app")
    
class Controller(vkt.Controller):
    parametrization = Parametrization(width=40)

    @APSView("Model Viewer", duration_guess=40)
    def show_cad_model(self, params, **kwargs):
        # 1. Get 3‑legged token from VIKTOR integration
        integration = vkt.external.OAuth2Integration("aps-integration-1")
        token = integration.get_access_token()
        cad_dict = Controller.get_cad_files_dict(params, **kwargs)
        if cad_dict:
            cad_urn = cad_dict[params.files]["urn"]
            viewer_urn = Controller.to_b64url(cad_urn)
            return APSresult(urn_b64=viewer_urn, token=token)
        return None

    def get_file_content(self, params, **kwargs):
        """Get raw binary content of the selected file."""
        token = get_aps_token()
        return aps_helpers.get_file_content(
            token=token,
            hub_name=params.hubs,
            project_name=params.project,
            subfolder_path=params.subfolder_path,
            file_name=params.files,
        )

    @staticmethod
    def store_file_in_app(params, **kwargs):
        token = get_aps_token()
        file_content = aps_helpers.get_file_content(
            token=token,
            hub_name=params.hubs,
            project_name=params.project,
            subfolder_path=params.subfolder_path,
            file_name=params.files,
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