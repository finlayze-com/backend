# frontend/main.py

import dash
import dash_bootstrap_components as dbc
from dash import html

from frontend.layout.live_layout import layout
from frontend.callbacks.live_callbacks import register_live_callbacks

# اپ Dash با Bootstrap theme روشن
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)

# تعیین layout صفحه
app.layout = layout

# ثبت callbackها
register_live_callbacks(app)

# اجرای لوکال (مناسب برای dev فقط)
if __name__ == "__main__":
    app.run(debug=True, port=8050)
