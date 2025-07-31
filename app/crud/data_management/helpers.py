import requests
import app.crud.data_management as datamanagement_core
from functools import lru_cache


@lru_cache(maxsize=32)
def get_all_folder_paths_with_ids(
    hub_id: str, project_id: str, token: str
) -> list[tuple[str, str]]:
    """
    Traverses all top folders recursively and returns a list of (path, folder_id) tuples.
    """
    all_paths = []
    top_folders_list = datamanagement_core.get_top_folders(hub_id, project_id, token)
    if top_folders_list and top_folders_list.data:
        for folder in top_folders_list.data:
            path = folder.attributes.displayName
            folder_id = folder.id
            all_paths.append((path, folder_id))
            all_paths.extend(_get_subfolder_paths(project_id, folder_id, token, path))
    return all_paths


@lru_cache(maxsize=32)
def get_items_in_folder(project_id: str, folder_id: str, token: str) -> list[str]:
    """
    Retrieves item names from a specific folder.
    """
    item_names = []
    contents = datamanagement_core.get_folder_contents(project_id, folder_id, token)
    if contents and contents.data:
        for item in contents.data:
            if item.type == "items":
                item_names.append(item.attributes.displayName)
    return item_names


def get_top_folder_names(hub_id, project_id, token) -> list[str]:
    """
    Returns a list of display names for the top-level folders of a project.
    """
    folders_list = datamanagement_core.get_top_folders(hub_id, project_id, token)
    return [folder.attributes.displayName for folder in folders_list.data]


@lru_cache(1)
def get_all_cad_file_from_hub(token) -> dict[str, dict[str, str]] | None:
    """
    Main function to walk through the Autodesk APS hub structure and print a tree view.
    """
    all_viwables: dict[str, dict[str, str]] = {}
    hubs = datamanagement_core.get_hubs(token)
    if not hubs or not hubs.data:
        print("No hubs found for this token.")
        return

    for hub in hubs.data:
        hub_name = hub.attributes.name
        hub_id = hub.id
        print(f"Hub: {hub_name} (ID: {hub_id})")

        projects = datamanagement_core.get_projects(hub_id, token)
        if projects and projects.data:
            for project in projects.data:
                project_name = project.attributes.name
                # project_id from get_projects already has the "b." prefix for ACC projects
                # No need to modify it for Data Management API calls
                project_id_with_prefix = project.id
                print(f"  Project: {project_name} (ID: {project_id_with_prefix})")

                # Use project_id_with_prefix for Data Management API calls
                top_folders = datamanagement_core.get_top_folders(
                    hub_id, project_id_with_prefix, token
                )
                if top_folders and top_folders.data:
                    for folder in top_folders.data:
                        viewables = get_all_cad_from_folder(
                            project_id_with_prefix, folder.id, token, indent="    "
                        )
                        if viewables:
                            all_viwables.update(viewables)
    return all_viwables


def get_all_cad_from_folder(project_id, folder_id, token, indent=""):
    """
    Recursively traverses a folder and its subfolders, printing contents.
    """
    viewable_files: dict[str, dict[str, str]] = {}
    try:
        contents = datamanagement_core.get_folder_contents(project_id, folder_id, token)
    except requests.exceptions.HTTPError as e:
        print(f"{indent}[Error accessing folder {folder_id}: {e}]")
        return viewable_files  # return an empty dict, not None

    if not contents.data:
        return viewable_files

    for content in contents.data:
        try:
            display_name = content.attributes.displayName
            content_type = content.type  # 'folders' or 'items'
            content_id = content.id

            print(f"{indent}{content_type.capitalize()[:-1]}: {display_name}")

            if content_type == "folders":
                # Capture and merge the returned data
                sub_viewables = get_all_cad_from_folder(
                    project_id, content_id, token, indent + "  "
                )
                if sub_viewables:
                    viewable_files.update(sub_viewables)

            elif content_type == "items":
                supported_extensions = [
                    ".rvt",
                    ".dwg",
                    ".ifc",
                    ".step",
                    ".stp",
                    ".iam",
                    ".ipt",
                ]
                if any(
                    display_name.lower().endswith(ext) for ext in supported_extensions
                ):
                    versions = datamanagement_core.get_item_versions(
                        project_id, content_id, token
                    )
                    if versions:
                        latest_version = versions[0]
                        version_urn = latest_version["id"]
                        print(f"{indent}  - Latest Version URN: {version_urn}")
                        viewable_files[display_name] = {"urn": version_urn}
                        model_views = datamanagement_core.get_model_views_and_metadata(
                            version_urn, token
                        )
                        if model_views:
                            for view in model_views:
                                print(
                                    f"{indent}    - View: {view.get('name')}, "
                                    f"GUID: {view.get('guid')}"
                                )
                else:
                    print(f"{indent}  - (Skipping derivative check for non‑CAD file)")

        except (requests.exceptions.HTTPError, AttributeError) as item_error:
            display_name_for_error = "Unknown"
            if hasattr(content, "attributes") and hasattr(
                content.attributes, "displayName"
            ):
                display_name_for_error = content.attributes.displayName
            print(
                f"{indent}  [Could not process item {display_name_for_error}: {item_error}]"
            )
            continue

    return viewable_files


def get_hub_names(token):
    """Return a list of hub names for the given token."""
    hubs = datamanagement_core.get_hubs(token)
    if hubs and hasattr(hubs, "data"):
        return [hub.attributes.name for hub in hubs.data]
    return []


def get_projects_names_by_hub(token, hub_name):
    """Return a list of project names for a given hub name and token."""
    hubs = datamanagement_core.get_hubs(token)
    hub_id = None
    if hubs and hasattr(hubs, "data"):
        for hub in hubs.data:
            if getattr(hub.attributes, "name", None) == hub_name:
                hub_id = hub.id
                break
    if not hub_id:
        return ["Hub not found"]
    projects_list = datamanagement_core.get_projects(hub_id=hub_id, token=token)
    if hasattr(projects_list, "data"):
        return [project.attributes.name for project in projects_list.data]
    return ["No projects found"]


def get_project_id_by_name(token, hub_id, project_name):
    """Return the project ID for a given project name in a hub."""
    projects_list = datamanagement_core.get_projects(hub_id=hub_id, token=token)
    if projects_list and hasattr(projects_list, "data"):
        for project in projects_list.data:
            if getattr(project.attributes, "name", None) == project_name:
                return project.id
    return None


def get_hub_id_by_name(token, hub_name):
    """Return hub ID for a given hub name."""
    hubs = datamanagement_core.get_hubs(token)
    if hubs and hasattr(hubs, "data"):
        for hub in hubs.data:
            if getattr(hub.attributes, "name", None) == hub_name:
                return hub.id
    return None


def get_hub_top_folders_names(token, hub_name, project_name):
    """Return a list of top folder display names for a given hub and project."""
    hub_id = get_hub_id_by_name(token, hub_name)
    project_id = get_project_id_by_name(token, hub_id, project_name)
    top_folders = datamanagement_core.get_top_folders(hub_id, project_id, token)
    if top_folders and hasattr(top_folders, "data"):
        return [folder.attributes.displayName for folder in top_folders.data]
    return []


def _get_subfolder_paths(
    project_id: str, parent_folder_id: str, token: str, current_path: str
) -> list[tuple[str, str]]:
    """Helper to recursively get sub-folder paths."""
    paths = []
    contents = datamanagement_core.get_folder_contents(
        project_id, parent_folder_id, token
    )
    if contents and contents.data:
        for item in contents.data:
            if item.type == "folders":
                folder_name = item.attributes.displayName
                folder_id = item.id
                new_path = f"{current_path}/{folder_name}"
                paths.append((new_path, folder_id))
                paths.extend(
                    _get_subfolder_paths(project_id, folder_id, token, new_path)
                )
    return paths


def get_subfolders_paths_from_top_folder(token, hub_name, project_name, top_folder):
    """Return all subfolder paths under the given top folder."""
    hub_id = get_hub_id_by_name(token, hub_name)
    project_id = get_project_id_by_name(token, hub_id, project_name)
    all_paths_with_ids = get_all_folder_paths_with_ids(hub_id, project_id, token) or []
    subfolder_paths = [
        path
        for path, _ in all_paths_with_ids
        if path != top_folder and path.startswith(f"{top_folder}/")
    ]
    return subfolder_paths or [f"No subfolders in {top_folder}"]


def get_files_names_from_sub_folder(token, hub_name, project_name, subfolder_path):
    """Return items for a given subfolder path."""
    hub_id = get_hub_id_by_name(token, hub_name)
    project_id = get_project_id_by_name(token, hub_id, project_name)
    if not project_id:
        return [f"Project not found for hub '{hub_name}' and project '{project_name}'"]
    all_paths_with_ids = get_all_folder_paths_with_ids(hub_id, project_id, token) or []
    folder_id = next(
        (fid for path, fid in all_paths_with_ids if path == subfolder_path), None
    )
    if not folder_id:
        return [f"Folder ID not found for {subfolder_path}"]
    items = get_items_in_folder(project_id, folder_id, token)
    return items or [f"No items in {subfolder_path}"]


def get_file_content(token: str, hub_name: str, project_name: str, subfolder_path: str, file_name: str) -> bytes:
    """
    Wrapper to get raw binary content of a file given navigation names.
    """
    hub_id = get_hub_id_by_name(token, hub_name)
    if not hub_id:
        raise ValueError(f"Could not find hub with name '{hub_name}'")
    project_id = get_project_id_by_name(token, hub_id, project_name)
    if not project_id:
        raise ValueError(f"Could not find project '{project_name}'")
    all_paths_with_ids = get_all_folder_paths_with_ids(hub_id, project_id, token)
    folder_id = next((fid for path, fid in all_paths_with_ids if path == subfolder_path), None)
    if not folder_id:
        raise ValueError(f"Could not find folder ID for path '{subfolder_path}'")
    contents = datamanagement_core.get_folder_contents(project_id, folder_id, token)
    item_id = None
    for item in contents.data:
        if item.type == "items" and item.attributes.displayName == file_name:
            item_id = item.id
            break
    if not item_id:
        raise ValueError(f"Could not find item ID for file '{file_name}'")
    versions = datamanagement_core.get_item_versions(project_id, item_id, token)
    if not versions:
        raise ValueError("No versions found for this item")
    latest_version = versions[0]
    storage_urn = latest_version.get("relationships", {}).get("storage", {}).get("data", {}).get("id")
    if not storage_urn:
        raise ValueError("Could not find storage location for this version")
    file_content = datamanagement_core.download_file_content(storage_urn, token)
    return file_content
