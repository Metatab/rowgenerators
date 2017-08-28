# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


class MetapackSource(SourceFile):
    def __init__(self, spec, dflo, cache, working_dir):
        super(MetapackSource, self).__init__(spec, dflo, cache)

    @property
    def package(self):
        from metatab import open_package
        return open_package(self.spec.resource_url, cache=self.cache)

    @property
    def resource(self):
        return self.package.resource(self.spec.target_segment)

    def __iter__(self):

        for row in self.resource:
            yield row

