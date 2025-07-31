from pathlib import Path
import viktor as vkt

class APSView(vkt.WebView):
    pass

class APSresult(vkt.WebResult):
    """Wrap ApsViewer.html and inject token + URN."""

    def __init__(self, urn_b64: str, token: str):
        html = (Path(__file__).parent / "ApsViewer.html").read_text()
        html = html.replace("APS_TOKEN_PLACEHOLDER", token)
        html = html.replace("URN_PLACEHOLDER", urn_b64)
        super().__init__(html=html)
