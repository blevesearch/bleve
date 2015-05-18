var TokenizerModalCtrl = function ($scope, $modalInstance, $http, name, value, mapping) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;

    $scope.tokenizer = {};
    // copy in value for editing
    for (var k in value) {
        $scope.tokenizer[k] = value[k];
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

    $scope.unknownTokenizerTypeTemplate = "/static/partials/analysis/tokenizers/generic.html";
    $scope.tokenizerTypeTemplates = {
        "regexp": "/static/partials/analysis/tokenizers/regexp.html",
        "exception": "/static/partials/analysis/tokenizers/exception.html"
    };
    $scope.tokenizerTypeDefaults = {
        "regexp": function() {
            return {
                "regexp": ""
            };
        },
        "exception": function() {
            return {
                "exceptions": [],
                "tokenizer": "unicode"
            };
        }
    };

    $scope.tokenizerTypes = [];

    updateTokenizerTypes = function() {
        $http.get('/api/_tokenizerTypes').success(function(data) {
            $scope.tokenizerTypes = data.tokenizer_types;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    updateTokenizerTypes();

    if (!$scope.tokenizer.type) {
        defaultType = "regexp";
        if ($scope.tokenizerTypeDefaults[defaultType]) {
            $scope.tokenizer = $scope.tokenizerTypeDefaults[defaultType]();
        }
        else {
            $scope.tokenizer = {};
        }
        $scope.tokenizer.type = defaultType;
    }
    $scope.formpath = $scope.tokenizerTypeTemplates[$scope.tokenizer.type];

    $scope.tokenizerTypeChange = function() {
        newType = $scope.tokenizer.type;
        if ($scope.tokenizerTypeDefaults[$scope.tokenizer.type]) {
            $scope.tokenizer = $scope.tokenizerTypeDefaults[$scope.tokenizer.type]();
        } else {
            $scope.tokenizer = {};
        }
        $scope.tokenizer.type = newType;
        if ($scope.tokenizerTypeTemplates[$scope.tokenizer.type]) {
            $scope.formpath = $scope.tokenizerTypeTemplates[$scope.tokenizer.type];
        } else {
            $scope.formpath = $scope.unknownTokenizerTypeTemplate;
        }
    };

    $scope.addException = function(scope) {
        if (scope.newregexp) {
            $scope.tokenizer.exceptions.push(scope.newregexp);
            scope.newregexp = "";
        }
    };

    $scope.removeException = function(index) {
        $scope.tokenizer.exceptions.splice(index, 1);
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
        if ($scope.name != $scope.origName && $scope.mapping.analysis.tokenizers[$scope.name]) {
            $scope.errorMessage = "Tokenizer named '" + $scope.name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        tokenizers = {};
        tokenizers[$scope.name] = $scope.tokenizer;
        // add in all the existing tokenizers, since we might be referencing them
        for (var t in $scope.mapping.analysis.tokenizers) {
            tokenizers[t] = $scope.mapping.analysis.tokenizers[t];
        }
        testMapping = {
            "analysis": {
                "tokenizers": tokenizers
            }
        };
        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[$scope.name] = $scope.tokenizer;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });

    };
};