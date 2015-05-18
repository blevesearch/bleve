// controller responsible for building a mapping

function MappingCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	newFieldSection = function() {
		return {
			"enabled": true,
			"dynamic": true,
			"default_analyzer": "",
			"properties": {},
			"fields": [
				{
					"type": "",
					"index": true,
					"store": true,
					"include_in_all": true,
					"include_term_vectors": true
				}
			]
		};
	};

	$scope.$parent.mapping = {
		"default_mapping": newFieldSection(),
		"type_field": "_type",
		"default_type": "_default",
		"default_analyzer": "standard",
		"default_datetime_parser": "dateTimeOptional",
		"default_field": "_all",
		"byte_array_converter": "json",
		"analysis": {
			"analyzers": {},
			"token_maps": {},
			"char_filters": {},
			"tokenizers": {},
			"token_filters": {}
		}
	};

	$scope.analyzerNames = [];

	$scope.loadAnalyzerNames = function() {
        $http.post('/api/_analyzerNames',$scope.$parent.mapping).success(function(data) {
            $scope.analyzerNames = data.analyzers;
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};

	$scope.loadAnalyzerNames();

	$scope.datetimeParserNames = [];

	$scope.loadDatetimeParserNames = function() {
        $http.post('/api/_datetimeParserNames',$scope.$parent.mapping).success(function(data) {
            $scope.datetimeParserNames = data.datetime_parsers;
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};

	$scope.loadDatetimeParserNames();

	$scope.mappingType = "default";
	$scope.selectedItem = null;
	$scope.selectedLabel = "";

	$scope.fieldTypes = [
		{
			"name": "text",
			"label": "Text",
			"description": "a text field"
		},
		{
			"name": "number",
			"label": "Number",
			"description": "a numerical value, indexed to facilitate range queries"
		},
		{
			"name": "datetime",
			"label": "Date/Time",
			"description": "a date/time value, indexed to facilitate range queries"
		},
		{
			"name": "disabled",
			"label": "Disabled",
			"description": "a section of JSON to be completely ignored"
		}
	];

	$scope.clickItem = function(x, y) {
		$scope.selectedItem = x;
		$scope.selectedLabel = y;
	};

	$scope.clickItem($scope.$parent.mapping.default_mapping);

	$scope.addField = function(scope) {
		if (scope.newFieldName) {
			$scope.selectedItem.properties[scope.newFieldName] = newFieldSection();
			scope.newFieldName = "";
			console.log($scope.selectedItem);
		}
	};

	$scope.changeType = function(scope) {
	};
}