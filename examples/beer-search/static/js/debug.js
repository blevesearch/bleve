function DebugCtrl($scope, $http, $routeParams, $log, $sce) {

	$scope.docid = "";
	$scope.maxKLen = 0;
	$scope.maxVLen = 0;

    $scope.debugDoc = function() {
        $http.get('/api/debug/'+$scope.docid).
        success(function(data) {
            $scope.processResults(data);
        }).
        error(function(data, code) {

        });
    };

    $scope.processResults = function(data) {
        $scope.errorMessage = null;
        $scope.results = data;

        for(var i in $scope.results) {
            row = $scope.results[i];
            row.k = atob(row.key).split('');
            if (row.k.length > $scope.maxKLen) {
				$scope.maxKLen = row.k.length;
            }
            row.ki = base64DecToArr(row.key);
            row.v = atob(row.val).split('');
            if (row.v.length > $scope.maxVLen) {
				$scope.maxVLen = row.v.length;
            }
        }
        $scope.klentimes = new Array($scope.maxKLen);
        $scope.vlentimes = new Array($scope.maxVLen);
    };

}