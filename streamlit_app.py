import streamlit as st

st.set_page_config(layout="wide")

# Lista de páginas
pages = [
    {"module": "views/app_gestion_de_insumos.py", "title": "Gestión de insumos", "icon": ":material/local_fire_department:"},
    # Agrega más páginas aquí
]

# Crear objetos de página
streamlit_pages = [
    st.Page(page["module"], title=page["title"], icon=page["icon"], default=page.get("default", False))
    for page in pages
]

# Agrupar páginas
pg = st.navigation({
    "Opciones": [streamlit_pages[0]],  # Inicio
})

# Ocultar el ícono de GitHub
st.markdown(
    """
    <style>
    .css-1jc7ptx, .e1ewe7hr3, .viewerBadge_container__1QSob,
    .styles_viewerBadge__1yB5_, .viewerBadge_link__1S137,
    .viewerBadge_text__1JaDK {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.logo("assets/logo.png")

pg.run()