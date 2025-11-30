import nox


@nox.session
def docs(session: nox.Session):
    session.install('sphinx', 'myst-parser', 'python-docs-theme', 'sphinx-autodoc2')
    session.run('python3', '-m', 'sphinx', *session.posargs)


@nox.session(
    python=[
        'python3.8',
        'python3.9',
        'python3.10',
        'python3.11',
        'python3.12',
        'python3.13',
    ]
)
def test(session: nox.Session):
    session.install(
        '.[orjson, adaptix, rabbitmq, msgpack]',
        'pytest', 'pytest-cov', 'pytest-asyncio'
    )
    session.run('pytest', *session.posargs)
