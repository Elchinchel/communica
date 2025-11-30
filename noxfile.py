from os import listdir
from tempfile import TemporaryDirectory

import nox


@nox.session(
        default=False
)
def docs(session: nox.Session):
    session.install('sphinx', 'myst-parser', 'python-docs-theme', 'sphinx-autodoc2')
    session.run('python3', '-m', 'sphinx', *session.posargs)


@nox.session(
    python=[
        'python3.9',
        'python3.10',
        'python3.11',
        'python3.12',
        'python3.13',
        'python3.14',
    ]
)
def test(session: nox.Session):
    session.install(
        '.[orjson, adaptix, rabbitmq, msgpack]',
        'pytest', 'pytest-cov', 'pytest-asyncio'
    )
    session.run('pytest', *session.posargs)


@nox.session(default=False)
def upload(session: nox.Session):
    session.install('build')

    with TemporaryDirectory() as dist_dir:
        session.run('pyproject-build', '--outdir', dist_dir)

        session.install('twine')
        with session.chdir(dist_dir):
            session.run('twine', 'upload', '--non-interactive', *listdir(dist_dir))
