import viktor as vkt # type: ignore
import os

from pathlib import Path
from tools import process_cad_file, get_token

class APSView(vkt.WebView):
    pass

class APSresult(vkt.WebResult):
    def __init__(self, file: vkt.File, name: str, client_id: str,  client_secret: str, bucket_id: str | None = None):
        file_bytes: bytes = file.getvalue_binary()
        
        token = get_token(client_id=client_id, client_secret=client_secret)
        urn = process_cad_file(
            file_content=file_bytes, object_name=name, token = token, client_id=client_id
        )
        
        html = (Path(__file__).parent / 'ApsViewer.html').read_text()
        html = html.replace('APS_TOKEN_PLACEHOLDER', token)
        html = html.replace('URN_PLACEHOLDER', urn)
        super().__init__(html=html)

class Parametrization(vkt.Parametrization):
    title = vkt.Text("# APS Integration")
    cad_file = vkt.FileField("Upload Your CAD File!")

class Controller(vkt.Controller):
    parametrization = Parametrization(width=40)

    @APSView("Model Viewer", duration_guess=40)
    def process_cadd_file(self, params, **kwargs) -> APSresult:
        
        file: vkt.File = params.cad_file.file
        name = params.cad_file.filename
        client_id = os.environ.get("CLIENT_ID")
        client_secret = os.environ.get("CLIENT_SECRET")

        if not client_id or not client_secret:
            raise vkt.UserError("CLIENT_ID and CLIENT_SECRET must be set in the environment variables.")
        
        return APSresult(file=file, name=name, client_id=client_id, client_secret=client_secret)
