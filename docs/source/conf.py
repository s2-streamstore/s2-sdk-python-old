# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

from datetime import date

project = "s2-sdk"
copyright = f"{date.today().year}, Bandar Systems Inc"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.autodoc.typehints",
    "sphinx.ext.intersphinx",
    "enum_tools.autoenum",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = []


autodoc_member_order = "bysource"
autodoc_typehints_format = "short"
python_use_unqualified_type_names = True

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
html_theme_options = {
    "light_logo": "s2-logo-black.png",
    "dark_logo": "s2-logo-white.png",
}
