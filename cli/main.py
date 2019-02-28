#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os

import click

from cli.tf.data_loader import TensorflowDataLoader
from neptune import Session


@click.group()
def main():
    pass


@main.command()
@click.option('--as-cowboy', '-c', is_flag=True, help='Greet as a cowboy.')
@click.argument('name', default='world', required=False)
def hello(name, as_cowboy):
    greet = 'Howdy' if as_cowboy else 'Hello'
    click.echo('{0}, {1}.'.format(greet, name))


@main.group('sync')
@click.option('--api-token', '-a', help='Neptune Authorization Token')
@click.option('--project', '-p', required=True, help='Project name')
@click.pass_context
def sync(ctx, api_token, project):
    ctx.obj = {'api_token': api_token, 'project': project}


@sync.command('tf')
@click.argument('path', required=True)
@click.pass_context
def load_tensorflow_data(ctx, path):
    session = Session(ctx.obj['api_token'])
    project = session.get_project(ctx.obj['project'])

    if not TensorflowDataLoader.requirements_installed():
        click.echo("ERROR: Package `tensorflow` is missing", err=True)
        return

    if not os.path.exists(path):
        click.echo("ERROR: Provided path doesn't exist", err=True)
        return

    loader = TensorflowDataLoader(project, path)
    loader.run()


if __name__ == '__main__':
    main()
