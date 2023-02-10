from datetime import datetime

import numpy
from PIL import Image

from neptune.legacy import Session
from tests.e2e.base import fake
from tests.e2e.utils import tmp_context


class TestLegacyClient:
    def test_experiment_creation(self, environment):
        session = Session(api_token=environment.user_token)
        project = session.get_project(project_qualified_name=environment.project)
        project.create_experiment()

    def test_tags_operations(self, environment):
        session = Session(api_token=environment.user_token)
        project = session.get_project(project_qualified_name=environment.project)
        experiment = project.create_experiment(
            tags=["initial tag 1", "initial tag 2"],
        )
        experiment.append_tags("tag1")
        experiment.append_tag(["tag2_to_remove", "tag3"])

        assert set(experiment.get_tags()) == {
            "initial tag 1",
            "initial tag 2",
            "tag1",
            "tag2_to_remove",
            "tag3",
        }

    def test_properties(self, environment):
        session = Session(api_token=environment.user_token)
        project = session.get_project(project_qualified_name=environment.project)
        experiment = project.create_experiment(
            params={
                "init_text_parameter": "some text",
                "init_number parameter": 42,
                "init_list": [1, 2, 3],
                "init_datetime": datetime.now(),
            },
        )
        experiment.set_property("prop", "some text")
        experiment.set_property("prop_number", 42)
        experiment.set_property("nested/prop", 42)
        experiment.set_property("prop_to_del", 42)
        experiment.set_property("prop_list", [1, 2, 3])

        experiment.set_property("prop_datetime", datetime.now())
        experiment.remove_property("prop_to_del")

        properties = experiment.get_properties()
        assert properties["prop"] == "some text"
        assert properties["prop_number"] == "42"
        assert properties["nested/prop"] == "42"
        assert "prop_to_del" not in properties

    def test_log_operations(self, environment):
        # given
        session = Session(api_token=environment.user_token)
        project = session.get_project(project_qualified_name=environment.project)
        experiment = project.create_experiment()

        # when
        experiment.log_metric("m1", 1)
        experiment.log_metric("m1", 2)
        experiment.log_metric("m1", 3)
        experiment.log_metric("m1", 2)
        experiment.log_metric("nested/m1", 1)

        # and
        experiment.log_text("m2", "a")
        experiment.log_text("m2", "b")
        experiment.log_text("m2", "c")

        # and
        with tmp_context():
            filename = fake.file_name(extension="png")

            tmp = numpy.random.rand(100, 100, 3) * 255
            im = Image.fromarray(tmp.astype("uint8")).convert("RGBA")
            im.save(filename)

            experiment.log_image("g_img", filename, image_name="name", description="desc")
            experiment.log_image("g_img", filename)

        # then
        logs = experiment.get_logs()
        assert "m1" in logs
        assert "m2" in logs
        assert "nested/m1" in logs
        assert "g_img" in logs

    def test_files_operations(self, environment):
        # given
        session = Session(api_token=environment.user_token)
        project = session.get_project(project_qualified_name=environment.project)
        experiment = project.create_experiment()

        # when
        # image
        with tmp_context():
            filename = fake.file_name(extension="png")

            tmp = numpy.random.rand(100, 100, 3) * 255
            im = Image.fromarray(tmp.astype("uint8")).convert("RGBA")
            im.save(filename)

            experiment.send_image("image", filename, name="name", description="desc")

        # artifact
        with tmp_context():
            filename = fake.file_name(extension="png")

            with open(filename, "wb") as file:
                file.write(fake.sentence().encode("utf-8"))

            experiment.send_artifact(filename)
            experiment.log_artifact(filename, destination="dir/text file artifact")

            with open(filename, mode="r") as f:
                experiment.send_artifact(f, destination="file stream.txt")

            experiment.log_artifact(filename, destination="dir to delete/art1")
            experiment.log_artifact(filename, destination="dir to delete/art2")

        experiment.delete_artifacts("dir to delete/art1")
