import json
from io import BytesIO
from pathlib import Path
from urllib.error import URLError

from streamlit.testing.v1 import AppTest


APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_dashboard_sends_features_and_displays_score(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured.update(json.loads(request.data))
        assert request.get_method() == "POST"
        assert request.full_url.endswith("/predict")
        assert timeout == 10
        result = {
            "purchase_within_7d_probability": 0.0166884,
            "timing_given_purchase": {
                "under_24h": 0.4751,
                "hours_24_to_72": 0.2671,
                "hours_72_to_168": 0.2578,
            },
        }
        return BytesIO(json.dumps(result).encode())

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    app = AppTest.from_file(APP).run()
    assert len(app.exception) == 0
    assert len(app.number_input) == 13

    values = [5, 2, 1, 1, 0, 1, 1, 2, 1, 0, 1, 2.5, 3.0]
    for widget, value in zip(app.number_input, values):
        widget.set_value(value)

    app.button[0].click().run()

    assert len(app.exception) == 0
    assert len(captured) == 13
    assert captured["events_previous_day"] == 5
    assert captured["hours_since_last_sku_event"] == 3.0
    assert app.metric[0].value == "1.67%"
    assert app.subheader[0].value == "Timing, conditional on a purchase"


def test_dashboard_shows_connection_error(monkeypatch):
    def unavailable(request, timeout):
        raise URLError("offline")

    monkeypatch.setattr("urllib.request.urlopen", unavailable)

    app = AppTest.from_file(APP).run()
    app.button[0].click().run()

    assert len(app.exception) == 0
    assert len(app.error) == 1
    assert len(app.metric) == 0