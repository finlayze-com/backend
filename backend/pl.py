import plotly.graph_objects as go
import pandas as pd

# ─── داده اولیه ───────────────────────────────────────────────
data = [
    {"Sector": "خودرو و ساخت قطعات", "net_real_flow": 10789133275646},
    {"Sector": "محصولات شیمیایی", "net_real_flow": 1073201530617},
    {"Sector": "فراورده های نفتی، کک و سوخت هسته ای", "net_real_flow": 966553511567},
    {"Sector": "فعالیتهای کمکی به نهادهای مالی واسط", "net_real_flow": 818504346930},
    {"Sector": "سرمایه گذاریها", "net_real_flow": 705628018097},
    {"Sector": "مواد و محصولات دارویی", "net_real_flow": 699589339210},
    {"Sector": "بانکها و موسسات اعتباری", "net_real_flow": 536252988187},
    {"Sector": "حمل ونقل، انبارداری و ارتباطات", "net_real_flow": 433410459635},
    {"Sector": "ماشین آلات و تجهیزات", "net_real_flow": 346817128645},
    {"Sector": "محصولات غذایی و آشامیدنی به جز قند و شکر", "net_real_flow": 288147962009},
    {"Sector": "استخراج زغال سنگ", "net_real_flow": 272810888730},
    {"Sector": "فلزات اساسی", "net_real_flow": 264809168684},
    {"Sector": "شرکتهای چند رشته ای صنعتی", "net_real_flow": 213185481701},
    {"Sector": "انبوه سازی، املاک و مستغلات", "net_real_flow": 177604130987},
    {"Sector": "ماشین آلات و دستگاههای برقی", "net_real_flow": 133753855366},
    {"Sector": "مخابرات", "net_real_flow": 121287357960},
    {"Sector": "ساخت محصولات فلزی", "net_real_flow": 82156525715},
    {"Sector": "سایر محصولات کانی غیرفلزی", "net_real_flow": 72200930175},
    {"Sector": "پیمانکاری صنعتی", "net_real_flow": 46854687272},
    {"Sector": "خدمات فنی و مهندسی", "net_real_flow": 38519684071},
    {"Sector": "تولید محصولات کامپیوتری الکترونیکی ونوری", "net_real_flow": 20656519390},
    {"Sector": "حمل و نقل آبی", "net_real_flow": 16798907918},
    {"Sector": "انتشار، چاپ و تکثیر", "net_real_flow": 9415326000},
    {"Sector": "محصولات کاغذی", "net_real_flow": 9354353122},
    {"Sector": "خرده فروشی،باستثنای وسایل نقلیه موتوری", "net_real_flow": 8304984724},
    {"Sector": "لاستیک و پلاستیک", "net_real_flow": 7564577797},
    {"Sector": "اطلاعات و ارتباطات", "net_real_flow": 6485092268},
    {"Sector": "دباغی، پرداخت چرم و ساخت انواع پاپوش", "net_real_flow": 2740700000},
    {"Sector": "محصولات چوبی", "net_real_flow": 2125862600},
    {"Sector": "ساخت دستگاهها و وسایل ارتباطی", "net_real_flow": 1199828420},
    {"Sector": "فعالیت مهندسی، تجزیه، تحلیل و آزمایش فنی", "net_real_flow": 0},
    {"Sector": "فعالیت های هنری، سرگرمی و خلاقانه", "net_real_flow": 0},
    {"Sector": "فعالیتهای فرهنگی و ورزشی", "net_real_flow": 0},
    {"Sector": "استخراج کانه های فلزی", "net_real_flow": -3739304738},
    {"Sector": "رایانه و فعالیتهای وابسته به آن", "net_real_flow": -8993138605},
    {"Sector": "تجارت عمده فروشی به جز وسایل نقلیه موتور", "net_real_flow": -10849000000},
    {"Sector": "کاشی و سرامیک", "net_real_flow": -21968770349},
    {"Sector": "سایر واسطه گریهای مالی", "net_real_flow": -28371216581},
    {"Sector": "منسوجات", "net_real_flow": -53141358692},
    {"Sector": "عرضه برق، گاز، بخاروآب گرم", "net_real_flow": -74252657330},
    {"Sector": "هتل و رستوران", "net_real_flow": -103535634376},
    {"Sector": "استخراج سایر معادن", "net_real_flow": -104173384510},
    {"Sector": "قند و شکر", "net_real_flow": -142798768455},
    {"Sector": "بیمه وصندوق بازنشستگی به جزتامین اجتماعی", "net_real_flow": -200682531668},
    {"Sector": "زراعت و خدمات وابسته", "net_real_flow": -410138226969},
    {"Sector": "استخراج نفت گاز و خدمات جنبی جز اکتشاف", "net_real_flow": -492197080900},
    {"Sector": "سیمان، آهک و گچ", "net_real_flow": -804233346840},
    {"Sector": "صندوق سرمایه گذاری قابل معامله", "net_real_flow": -2418192343954}
]

df = pd.DataFrame(data)

# ─── پردازش داده‌ها ────────────────────────────────────────────
df = df[df["net_real_flow"] != 0]
df["net_real_flow_abs"] = df["net_real_flow"].abs()

# جدا کردن جریان‌های ورودی و خروجی
inflow_df = df[df["net_real_flow"] > 0]
outflow_df = df[df["net_real_flow"] < 0]

# ─── ساخت گره‌ها (labels) و ایندکس‌دهی ─────────────────────────
nodes = ["بازار"] + inflow_df["Sector"].tolist() + outflow_df["Sector"].tolist()
node_map = {name: idx for idx, name in enumerate(nodes)}

# ─── ساخت لینک‌ها ───────────────────────────────────────────────
links = []

# از "بازار" به inflow (مثبت)
for _, row in inflow_df.iterrows():
    links.append(dict(
        source=node_map["بازار"],
        target=node_map[row["Sector"]],
        value=row["net_real_flow_abs"]
    ))

# از outflow (منفی) به "بازار"
for _, row in outflow_df.iterrows():
    links.append(dict(
        source=node_map[row["Sector"]],
        target=node_map["بازار"],
        value=row["net_real_flow_abs"]
    ))

# ─── رسم دیاگرام Sankey ─────────────────────────────────────────
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=20,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=nodes,
        color=["#636EFA"] + ["#00CC96"] * len(inflow_df) + ["#EF553B"] * len(outflow_df)
    ),
    link=dict(
        source=[l["source"] for l in links],
        target=[l["target"] for l in links],
        value=[l["value"] for l in links],
    )
)])

fig.update_layout(
    title="Sankey جریان نقدینگی حقیقی بین بازار و صنایع",
    font=dict(family="Vazirmatn", size=13)
)

fig.show()
