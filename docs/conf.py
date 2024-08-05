# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import datetime


year = datetime.datetime.now().year


project = 'Communica'
copyright = f'{year}, P.I.V.O.'
author = 'Elchinchel'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_parser',
    'autodoc2',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


autodoc2_packages = [
    "../communica",
]
autodoc2_hidden_objects = ['private', 'inherited']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_logo = '_static/servershake.svg'
html_theme = 'python_docs_theme'
html_static_path = ['_static']
