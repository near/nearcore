#!/bin/python3

# This script checks that nightly feature flags are declared consistently so
# we avoid nasty subtle bugs. See the individual checks below for details.

import toml
import re
import difflib
import sys


class Crate:

    def __init__(self, dir):
        self.dir = dir
        self.height = None
        self.load_toml()

    def load_toml(self):
        self.toml = toml.load(self.dir + "/Cargo.toml")
        self.name = self.toml["package"]["name"]
        self.features = self.toml.get("features", {})

        self.has_nightly_feature = "nightly" in self.features
        self.has_nightly_protocol_feature = "nightly_protocol" in self.features
        self.local_nightly_features = self.get_local_nightly_features()
        self.local_nightly_protocol_features = self.get_local_nightly_protocol_features(
        )
        self.dependency_nightly_features = self.get_dependency_nightly_features(
        )
        self.dependency_nightly_protocol_features = self.get_dependency_nightly_protocol_features(
        )

    def get_local_nightly_features(self):
        if not self.has_nightly_feature:
            return []
        return [
            feature for feature in self.features["nightly"]
            if '/' not in feature
        ]

    def get_local_nightly_protocol_features(self):
        if not self.has_nightly_protocol_feature:
            return []
        return [
            feature for feature in self.features["nightly_protocol"]
            if '/' not in feature
        ]

    def get_dependency_nightly_features(self):
        if not self.has_nightly_feature:
            return []
        return [
            feature for feature in self.features["nightly"] if '/' in feature
        ]

    def get_dependency_nightly_protocol_features(self):
        if not self.has_nightly_protocol_feature:
            return []
        return [
            feature for feature in self.features["nightly_protocol"]
            if '/' in feature
        ]

    def build_deps(self, crates_by_name):
        self.deps = [
            crates_by_name[dep]
            for dep in self.toml['dependencies'].keys()
            if dep in crates_by_name
        ]

    def build_transitive_deps(self):
        visited = set()
        result = []

        def recursion_helper(crate):
            result.append(crate)
            for dep in crate.deps:
                if dep.name not in visited:
                    visited.add(dep.name)
                    recursion_helper(dep)

        for dep in self.deps:
            if dep.name not in visited:
                visited.add(dep.name)
                recursion_helper(dep)
        self.transitive_deps = result

    def write_toml(self, apply_fix):
        toml_file_name = self.dir + "/Cargo.toml"
        existing_toml_text = open(toml_file_name, "r").read()
        new_toml_text = existing_toml_text

        def ensure_features():
            nonlocal new_toml_text
            if "[features]" not in new_toml_text:
                new_toml_text += "\n[features]\nnightly = []\nnightly_protocol = []\n"
            if "nightly =" not in new_toml_text:
                new_toml_text = re.sub(r"\[features\]",
                                       "[features]\nnightly = []",
                                       new_toml_text)
            if "nightly_protocol =" not in new_toml_text:
                new_toml_text = re.sub(r"\[features\]",
                                       "[features]\nnightly_protocol = []",
                                       new_toml_text)

        def replace_nightly(deps):
            nonlocal new_toml_text
            desired_text = 'nightly = [\n'
            for dep in deps:
                desired_text += '  "{}",\n'.format(dep)
            desired_text += ']'
            regex = re.compile(r"nightly\s*=\s*\[.*?\]", re.DOTALL)
            (new_toml_text, n) = re.subn(regex, desired_text, new_toml_text)
            assert n == 1, "Substitution failed for crate {}".format(self.name)

        def replace_nightly_protocol(deps):
            nonlocal new_toml_text
            desired_text = 'nightly_protocol = [\n'
            for dep in deps:
                desired_text += '  "{}",\n'.format(dep)
            desired_text += ']'
            regex = re.compile(r"nightly_protocol\s*=\s*\[.*?\]", re.DOTALL)
            (new_toml_text, n) = re.subn(regex, desired_text, new_toml_text)
            assert n == 1, "Substitution failed for crate {}".format(self.name)

        if self.has_nightly_feature:
            ensure_features()
            replace_nightly(
                list(sorted(self.local_nightly_features)) +
                list(sorted(self.dependency_nightly_features)))
        if self.has_nightly_protocol_feature:
            ensure_features()
            replace_nightly_protocol(
                list(sorted(self.local_nightly_protocol_features)) +
                list(sorted(self.dependency_nightly_protocol_features)))

        if apply_fix:
            open(toml_file_name, "w").write(new_toml_text)
        else:
            if new_toml_text != existing_toml_text:
                print("Nightly feature flags need updating:")
                sys.stdout.writelines(
                    difflib.unified_diff(
                        existing_toml_text.splitlines(keepends=True),
                        new_toml_text.splitlines(keepends=True),
                        fromfile=toml_file_name,
                        tofile=toml_file_name))
                return False
            return True


workspace_toml = toml.load("Cargo.toml")
crate_dirs = workspace_toml["workspace"]["members"]

crates = [Crate(dir) for dir in crate_dirs]
crate_by_name = {crate.name: crate for crate in crates}
for crate in crates:
    crate.build_deps(crate_by_name)
for crate in crates:
    crate.build_transitive_deps()

for crate in crates:
    # check 1: we should either have neither feature, or both.
    if crate.has_nightly_feature != crate.has_nightly_protocol_feature:
        if crate.has_nightly_feature:
            print(
                "crate {} has nightly feature but not nightly_protocol. Adding nightly_protocol"
                .format(crate.name))
            crate.has_nightly_protocol_feature = True
            crate.local_nightly_features.append('nightly_protocol')
        else:
            print(
                "crate {} has nightly_protocol feature but not nightly. Adding nightly"
                .format(crate.name))
            crate.has_nightly_feature = True
            crate.local_nightly_features.append('nightly_protocol')

    # check 2: nightly_protocol should not activate on local features.
    if crate.local_nightly_protocol_features != []:
        print(
            "crate {} has local nightly_protocol features: {}, moving to nightly"
            .format(crate.name, crate.local_nightly_protocol_features))
        crate.local_nightly_features += crate.local_nightly_protocol_features
        crate.local_nightly_protocol_features = []

    # check 3: nightly should activate local nightly_protocol.
    if crate.has_nightly_feature:
        if 'nightly_protocol' not in crate.local_nightly_features:
            print(
                "crate {} nightly feature does not include nightly_protocol. Adding nightly_protocol"
                .format(crate.name))
            crate.local_nightly_features.append('nightly_protocol')

while True:
    fixed = False
    for crate in crates:
        # check 4: nightly should be present if there's a transitive dependency that has it and there's also a transitive dependent that
        # has it. Without this, suppose A depends on B and B depends on C, but B does not declare a nightly feature. There would be no way
        # for A's nightly feature to activate C's nightly feature, since C is not a direct dependency of A. So to avoid that kind of headache
        # we require B to also declare nightly.
        transitive_dependencies_with_nightly = [
            dep for dep in crate.transitive_deps if dep.has_nightly_feature
        ]
        dependents_with_nightly = [
            other for other in crates
            if crate in other.deps and other.has_nightly_feature
        ]
        if transitive_dependencies_with_nightly != [] and dependents_with_nightly != [] and crate.has_nightly_feature == False:
            print(
                "crate {} has both dependencies (e.g. {}) with nightly and dependents (e.g. {}) with nightly but does not have nightly feature itself. Adding nightly feature"
                .format(crate.name,
                        transitive_dependencies_with_nightly[0].name,
                        dependents_with_nightly[0].name))
            crate.has_nightly_feature = True
            crate.has_nightly_protocol_feature = True
            crate.local_nightly_features.append('nightly_protocol')
            fixed = True
    if not fixed:
        break

for crate in crates:
    if not crate.has_nightly_feature:
        continue
    # check 5: nightly should activate precisely the nightly flags of all direct dependencies.
    # Same with nightly_protocol.
    dependencies_with_nightly = [
        dep for dep in crate.deps if dep.has_nightly_feature
    ]
    desired_dependency_features = [
        dep.name + '/nightly' for dep in dependencies_with_nightly
    ]
    if set(crate.dependency_nightly_features) != set(
            desired_dependency_features):
        print("crate {} has nightly dependencies {} but should be {}, fixing".
              format(crate.name, crate.dependency_nightly_features,
                     desired_dependency_features))
        crate.dependency_nightly_features = desired_dependency_features

    dependencies_with_nightly_protocol = [
        dep for dep in crate.deps if dep.has_nightly_protocol_feature
    ]
    desired_dependency_features = [
        dep.name + '/nightly_protocol'
        for dep in dependencies_with_nightly_protocol
    ]
    if set(crate.dependency_nightly_protocol_features) != set(
            desired_dependency_features):
        print(
            "crate {} has nightly_protocol dependencies {} but should be {}, fixing"
            .format(crate.name, crate.dependency_nightly_protocol_features,
                    desired_dependency_features))
        crate.dependency_nightly_protocol_features = desired_dependency_features

    # check 6: if any feature declared locally has a name that coincides with a nightly
    # feature of a transitive dependency, that feature should also be included in the
    # local nightly list. Otherwise, this is likely a mistake, because nightly would
    # activate a feature in a dependency crate but not a dependent crate.
    all_dependency_local_nightly_features = []
    for dep in crate.transitive_deps:
        all_dependency_local_nightly_features += dep.local_nightly_features
    for feature in crate.features.keys():
        if feature == 'nightly' or feature == 'nightly_protocol':
            continue
        if feature in all_dependency_local_nightly_features:
            if feature not in crate.local_nightly_features:
                print(
                    "crate {} has local feature {} that coincides with a dependency nightly feature. Adding to local nightly list"
                    .format(crate.name, feature))
                crate.local_nightly_features.append(feature)

    # TODO: there's a similar check we should do that checks if A depends on B and they
    # both have the same nightly feature name, then that feature of A should activate
    # that feature of B.

apply_fix = len(sys.argv) == 2 and sys.argv[1] == 'fix'
if apply_fix:
    for crate in crates:
        crate.write_toml(True)
else:
    all_correct = True
    for crate in crates:
        all_correct = all_correct and crate.write_toml(False)
    if not all_correct:
        print("Run the following to apply nightly feature flag fixes:")
        print("  python3 scripts/fix_nightly_feature_flags.py fix")
        sys.exit(1)
