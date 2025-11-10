import hopsworks

# Group name,version
group = tuple[str, int]


class Project:
    def __init__(self, name, engine="python"):
        self.project_name = name
        self.project = hopsworks.login(engine=engine, project=name)

    @property
    def feature_store(self):
        return self.project.get_feature_store()

    @property
    def model_registry(self):
        return self.project.get_model_registry()
    
    @property
    def feature_api(self):
        return self.project.get_feature_api()

    def get_feature_groups(self, groups: list[group] | None = None) -> tuple:
        """Gets a sequence of feature groups by their names and versions.

        Arguments:
            groups: list of (name: str, version: int) tuples

        Returns:
            tuple of FeatureGroup objects
        """
        return tuple(
            self.feature_store.get_feature_group(
                name=name,
                version=version,
            )
            for (name, version) in groups
        )
