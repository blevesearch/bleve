var CharFilterModalCtrl = function ($scope, $modalInstance, $http, name, value, mapping) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;

    $scope.charfilter = {};
    // copy in value for editing
    for (var k in value) {
        $scope.charfilter[k] = value[k];
    }

    $scope.unknownCharFilterTypeTemplate = "/static/partials/analysis/charfilters/generic.html";
    $scope.charFilterTypeTemplates = {
        "regexp": "/static/partials/analysis/charfilters/regexp.html",
    };
    $scope.charFilterTypeDefaults = {
        "regexp": function() {
            return {
                "regexp": "",
                "replace": ""
            };
        }
    };

    $scope.charFilterTypes = [];

    updateCharFilterTypes = function() {
        $http.get('/api/_charFilterTypes').success(function(data) {
            $scope.charFilterTypes = data.char_filter_types;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    updateCharFilterTypes();

    if (!$scope.charfilter.type) {
        defaultType = "regexp";
        if ($scope.charFilterTypeDefaults[defaultType]) {
            $scope.charfilter = $scope.charFilterTypeDefaults[defaultType]();
        }
        else {
            $scope.charfilter = {};
        }
        $scope.charfilter.type = defaultType;
        
    }
    $scope.formpath = $scope.charFilterTypeTemplates[$scope.charfilter.type];

    $scope.charFilterTypeChange = function() {
        newType = $scope.charfilter.type;
        if ($scope.charFilterTypeDefaults[$scope.charfilter.type]) {
            $scope.charfilter = $scope.charFilterTypeDefaults[$scope.charfilter.type]();
        } else {
            $scope.charfilter = {};
        }
        $scope.charfilter.type = newType;
        if ($scope.charFilterTypeTemplates[$scope.charfilter.type]) {
            $scope.formpath = $scope.charFilterTypeTemplates[$scope.charfilter.type];
        } else {
            $scope.formpath = unknownCharFilterTypeTemplate;
        }
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
        if ($scope.name != $scope.origName && $scope.mapping.analysis.char_filters[$scope.name]) {
            $scope.errorMessage = "Character filter named '" + $scope.name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        charFilters = {};
        charFilters[$scope.name] = $scope.charfilter;
        testMapping = {
            "analysis": {
                "char_filters": charFilters
            }
        };
        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[$scope.name] = $scope.charfilter;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });

    };
};