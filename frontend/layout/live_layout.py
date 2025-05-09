from dash import html, dcc
import dash_bootstrap_components as dbc

# ───────────────────── SIDEBAR ─────────────────────
sidebar = dbc.Col(
    [
        html.H4("🔍 جریان نقدینگی حقیقی", className="text-center mb-4", style={"color": "white"}),

        html.Label("🎯 انتخاب صنعت:", style={"color": "white"}),
        dcc.Dropdown(
            id="sector-dropdown",
            options=[],  # در حال حاضر برای intra-sector غیرفعال ولی باید باشه
            value=None,
            placeholder="یک صنعت انتخاب کنید...",
            style={"color": "black", "margin-bottom": "10px"},
        ),

        html.Div("🔀 حالت نمایش Sankey", style={"color": "white", "margin-bottom": "8px"}),
        dcc.RadioItems(
            id='sankey-mode',
            options=[
                {'label': 'بین صنایع', 'value': 'sector'},
                {'label': 'درون یک صنعت', 'value': 'intra-sector'}
            ],
            value='sector',
            labelStyle={'display': 'block', 'color': 'white'},
            inputStyle={'margin-right': '8px', 'margin-left': '4px'},
            style={"margin-bottom": "20px"}
        ),

        dbc.Button("اعمال تغییرات", id="apply-changes-btn", color="primary", className="w-100"),
    ],
    width=2,
    style={
        "background-color": "#1e293b",
        "padding": "20px",
        "height": "100vh",
        "position": "fixed",
        "top": 0,
        "left": 0,
        "overflow-y": "auto",
        "border-right": "2px solid #333",
        "zIndex": 1000,
        "width": "260px"
    }
)

# ───────────────────── MAIN CONTENT ─────────────────────
main_content = dbc.Col(
    [
        html.H2("📡 نمودار Sankey - بین صنایع", className="text-center my-4"),
        dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id="live-sankey"), type="default"), width=12)
        ]),
    ],
    width=10,
    style={"marginLeft": "260px", "padding": "20px"}
)

# ───────────────────── FINAL LAYOUT ─────────────────────
layout = dbc.Container(
    dbc.Row([
        sidebar,
        main_content
    ]),
    fluid=True
)
