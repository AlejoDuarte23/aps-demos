import base64
import urllib.parse
import viktor as vkt  # type: ignore
import app.crud.data_management as aps_helpers  
from app.views.apsView import APSView, APSresult

def get_aps_token():
    integration = vkt.external.OAuth2Integration("aps-integration-1")
    return integration.get_access_token()

def get_hub_list(params, **kwargs):
    token = get_aps_token()
    hub_names = aps_helpers.get_hub_names(token)
    return hub_names if hub_names else ["No hubs found"]

def get_projects(params, **kwargs) -> list[str]:
    if params.hubs:
        token = get_aps_token()
        hub_name = params.hubs
        projects = aps_helpers.get_projects_names_by_hub(token, hub_name)
        return projects
    return ["Select Hub First"]

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

class Controller(vkt.Controller):
    parametrization = Parametrization(width=40)

    @APSView("Model Viewer", duration_guess=40)
    def show_cad_model(self, params, **kwargs):
        # 1. Get 3‑legged token from VIKTOR integration
        integration = vkt.external.OAuth2Integration("aps-integration-1")
        token = integration.get_access_token()
        # 2. Log token payload once (scopes must include viewables:read)
        cad_dict = Controller.get_cad_files_dict(params, **kwargs)
        if cad_dict:
            cad_urn = cad_dict[params.files]["urn"]
            
            # Get and print file content
            try:
                file_content = self.get_file_content(params, **kwargs)
                # Print first 500 bytes to avoid console overflow
                print(f"File content preview: {file_content[:500]}")
            except Exception as e:
                print(f"Error retrieving file content: {str(e)}")
                
            viewer_urn = Controller.to_b64url(cad_urn)
            return APSresult(urn_b64=viewer_urn, token=token)
        return None
        
    def get_file_content(self, params, **kwargs):
        """Get raw binary content of the selected file."""
        token = get_aps_token()
        
        # Get necessary IDs
        hub_name = params.hubs
        project_name = params.project
        
        hub_id = aps_helpers.get_hub_id_by_name(token, hub_name)
        if not hub_id:
            raise ValueError(f"Could not find hub with name '{hub_name}'")
            
        project_id = aps_helpers.get_project_id_by_name(token, hub_id, project_name)
        if not project_id:
            raise ValueError(f"Could not find project '{project_name}'")
        
        # Get the item ID for the selected file
        subfolder_path = params.subfolder_path
        file_name = params.files
        
        # Get folder ID for the subfolder path
        all_paths_with_ids = aps_helpers.get_all_folder_paths_with_ids(hub_id, project_id, token)
        folder_id = next((fid for path, fid in all_paths_with_ids if path == subfolder_path), None)
        if not folder_id:
            raise ValueError(f"Could not find folder ID for path '{subfolder_path}'")
            
        # Get contents of the folder to find the item
        contents = aps_helpers.get_folder_contents(project_id, folder_id, token)
        item_id = None
        for item in contents.data:
            if item.type == "items" and item.attributes.displayName == file_name:
                item_id = item.id
                break
                
        if not item_id:
            raise ValueError(f"Could not find item ID for file '{file_name}'")
            
        # Get versions of the item
        versions = aps_helpers.get_item_versions(project_id, item_id, token)
        if not versions:
            raise ValueError("No versions found for this item")
            
        # Get the storage URN from the latest version
        latest_version = versions[0]
        storage_urn = latest_version.get("relationships", {}).get("storage", {}).get("data", {}).get("id")
        if not storage_urn:
            raise ValueError("Could not find storage location for this version")
            
        # Download the file content
        file_content = aps_helpers.download_file_content(storage_urn, token)
        return file_content

    @staticmethod
    def get_cad_files_dict(params, **kwargs) -> None | dict[str, dict[str, str]]:
        token = get_aps_token()
        viewables = aps_helpers.get_all_cad_file_from_hub(token=token)
        return viewables

    @staticmethod
    def to_b64url(value: str) -> str:
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")