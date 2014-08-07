'use strict';


// Declare app level module which depends on filters, and services
angular.module('myApp', [
  'ngRoute',
  'myApp.filters',
  'myApp.services',
  'myApp.directives',
  'myApp.controllers'
]).
config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider) {
  $routeProvider.when('/overview', {templateUrl: '/static/partials/overview.html', controller: 'MyCtrl1'});
  $routeProvider.when('/search/term/', {templateUrl: '/static/partials/search/term.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/match/', {templateUrl: '/static/partials/search/match.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/phrase/', {templateUrl: '/static/partials/search/phrase.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/match_phrase/', {templateUrl: '/static/partials/search/match_phrase.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/boolean/', {templateUrl: '/static/partials/search/boolean.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/syntax/', {templateUrl: '/static/partials/search/syntax.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/numeric_range/', {templateUrl: '/static/partials/search/numeric_range.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/date_range/', {templateUrl: '/static/partials/search/date_range.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/prefix/', {templateUrl: '/static/partials/search/prefix.html', controller: 'SearchCtrl'});
  $routeProvider.when('/search/debug/', {templateUrl: '/static/partials/debug.html', controller: 'DebugCtrl'});
  $routeProvider.otherwise({redirectTo: '/overview'});
  $locationProvider.html5Mode(true);
}]);
