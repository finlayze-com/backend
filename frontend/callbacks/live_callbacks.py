from dash import Input, Output
import pandas as pd
import plotly.graph_objects as go
from frontend.services.api_fetcher import get_sector_net_real_flow
from dash.exceptions import PreventUpdate
import dash


def draw_sector_sankey(df):
    if df.empty:
        return go.Figure()

    pos = df[df['net_real_flow'] > 0].copy()
    neg = df[df['net_real_flow'] < 0].copy()

    # تبدیل جریان‌ها به مقدار مطلق
    pos['net_real_flow'] = pos['net_real_flow'].abs()
    neg['net_real_flow'] = neg['net_real_flow'].abs()

    # ✳️ تراز جریان‌ها با نود "Other"
    total_pos = pos['net_real_flow'].sum()
    total_neg = neg['net_real_flow'].sum()

    if total_pos > total_neg:
        diff = total_pos - total_neg
        other_row = pd.DataFrame([{'Sector': 'Other', 'net_real_flow': diff}])
        neg = pd.concat([neg, other_row], ignore_index=True)
    elif total_neg > total_pos:
        diff = total_neg - total_pos
        other_row = pd.DataFrame([{'Sector': 'Other', 'net_real_flow': diff}])
        pos = pd.concat([pos, other_row], ignore_index=True)

    # ساخت نودها
    all_nodes = list(neg['Sector']) + list(pos['Sector'])
    node_indices = {sector: i for i, sector in enumerate(all_nodes)}

    links = []
    values = []

    for _, nrow in neg.iterrows():
        for _, prow in pos.iterrows():
            source = node_indices[nrow['Sector']]
            target = node_indices[prow['Sector']]
            flow = min(
                nrow['net_real_flow'] * (prow['net_real_flow'] / pos['net_real_flow'].sum()),
                prow['net_real_flow']
            )
            links.append(dict(source=source, target=target, value=flow))
            values.append(flow)

    # رنگ‌دهی
    node_colors = []
    for sector in all_nodes:
        if sector == 'Other':
            node_colors.append('rgba(150,150,150,0.4)')
        elif sector in list(neg['Sector']):
            node_colors.append('rgba(192,57,43,0.8)')
        else:
            node_colors.append('rgba(39,174,96,0.8)')

    # لیبل‌ها
    label_map = pd.concat([neg, pos], ignore_index=True).drop_duplicates(subset='Sector')
    label_dict = dict(zip(label_map['Sector'], label_map['net_real_flow']))

    labels = [f"{label}\n{label_dict.get(label, 0):,.0f}" for label in all_nodes]

    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=30,
            thickness=25,
            label=labels,
            color=node_colors,
            hovertemplate='%{label}<extra></extra>',
        ),
        link=dict(
            source=[l['source'] for l in links],
            target=[l['target'] for l in links],
            value=values,
            color='rgba(150,150,150,0.3)',
            hovertemplate="جریان: %{value:,.0f}<extra></extra>"
        )
    ))

    fig.update_layout(
        title="📊 جریان نقدینگی حقیقی بین صنایع",
        font_size=13,
        height=800,
        width=700,
        margin=dict(t=50, l=10, r=10, b=10),
        paper_bgcolor="#f9f9f9"
    )

    return fig


def register_live_callbacks(app):
    @app.callback(
        Output("live-sankey", "figure"),
        Input("apply-changes-btn", "n_clicks"),
        Input("sankey-mode", "value"),
        Input("sector-dropdown", "value")
    )
    def update_sankey(n, mode, selected_sector):
        print("✅ Sankey callback called", n)
        if mode != "sector":
            raise PreventUpdate

        df = get_sector_net_real_flow()
        return draw_sector_sankey(df)

