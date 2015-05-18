var AnalyzerModalCtrl = function ($scope, $modalInstance, $http, name, value, mapping) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;

    $scope.analyzer = {};
    // copy in value for editing
    for (var k in value) {
        // need deeper copy of nested arrays
        if (k == "char_filters") {
            newcharfilters = [];
            for (var cfi in value.char_filters) {
                newcharfilters.push(value.char_filters[cfi]);
            }
            $scope.analyzer.char_filters = newcharfilters;
        } else if (k == "token_filters") {
            newtokenfilters = [];
            for (var tfi in value.token_filters) {
                newtokenfilters.push(value.token_filters[tfi]);
            }
            $scope.analyzer.token_filters = newtokenfilters;
        } else {
            $scope.analyzer[k] = value[k];
        }
    }

    $scope.tokenizerNames = [];

    $scope.loadTokenizerNames = function() {
        $http.post('/api/_tokenizerNames',mapping).success(function(data) {
            $scope.tokenizerNames = data.tokenizers;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenizerNames();

    $scope.charFilterNames = [];

    $scope.loadCharFilterNames = function() {
        $http.post('/api/_charFilterNames',mapping).success(function(data) {
            $scope.charFilterNames = data.char_filters;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.loadCharFilterNames();

    $scope.addCharFilter = function(scope) {
        filter = scope.addCharacterFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.selectedAnalyzer.char_filters.push(filter);
        }
        console.log($scope.selectedAnalyzer.char_filters);
    };

    $scope.removeCharFilter = function(index) {
        $scope.selectedAnalyzer.char_filters.splice(index, 1);
    };

    $scope.tokenFilterNames = [];

    $scope.loadTokenFilterNames = function() {
        $http.post('/api/_tokenFilterNames',mapping).success(function(data) {
            $scope.tokenFilterNames = data.token_filters;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenFilterNames();

    $scope.addCharFilter = function(scope) {
        filter = scope.addCharacterFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.analyzer.char_filters.push(filter);
        }
        console.log($scope.analyzer.char_filters);
    };

    $scope.removeCharFilter = function(index) {
        $scope.analyzer.char_filters.splice(index, 1);
    };

    $scope.addTokenFilter = function(scope) {
        filter = scope.addTokenFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.analyzer.token_filters.push(filter);
        }
        console.log($scope.analyzer.token_filters);
    };

    $scope.removeTokenFilter = function(index) {
        $scope.analyzer.token_filters.splice(index, 1);
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.build = function() {
        // must have a name
        if (!$scope.name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if ($scope.name != $scope.origName && $scope.mapping.analysis.analyzers[$scope.name]) {
            $scope.errorMessage = "Analyzer named '" + $scope.name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        analysis = {};
        for (var ak in $scope.mapping.analysis) {
            analysis[ak] = $scope.mapping.analysis[ak];
        }
        analyzers = {};
        analyzers[$scope.name] = $scope.analyzer;
        analysis["analyzers"] = analyzers;
        testMapping = {
            "analysis": analysis
        };
        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[$scope.name] = $scope.analyzer;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });

    };
};