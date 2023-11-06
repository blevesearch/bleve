<h2>Purpose of Docvalues</h2>

<h3>Background</h3>

<p align="justify">What are docValues? In the index mapping, there is an option to enable or disable docValues for a specific field mapping. However, what does it actually mean to activate or deactivate docValues, and how does it impact the end user? This document aims to address these questions.</p>
<pre>
	"default_mapping": {
		"dynamic": true,
		"enabled": true,
		"properties": {
			"loremIpsum": {
			"enabled": true,
			"dynamic": false,
			"fields": [
				{
					"name": "loremIpsum",
					"type": "text",
					"store": false,
					"index": true,
					"include_term_vectors": false,
					"include_in_all": false,
					"docvalues": true
				}
			]
		}
	}
</pre>
<p align="justify">Enabling docValues will always result in an increase in the size of your Bleve index, leading to a corresponding increase in disk usage. But what advantages can you expect in return? This document also quantitatively assesses this trade-off with a test case.</p>

<p align="justify">In a more general sense, we recommend enabling docValues on a field mapping if you anticipate queries that involve sorting and/or facet operations on that field. It's important to note, though, that sorting and faceting will work irrespective of whether docValues are enabled or not. This may lead you to wonder if there's any real benefit to enabling docValues since you're allocating extra disk space without an apparent return. The real advantage, however, becomes evident in enhanced query response times and reduced memory consumption during active usage. By accepting a minor increase in the disk space used by your Full-Text Search (FTS) index, you can anticipate better performance in handling search requests that involve sorting and faceting.</p>

<h3>Usage</h3>

<p align="justify">The initial use of docValues comes into play when sorting is involved. In the search request JSON, there is a field named "sort." This optional "sort" field can have a slice of JSON objects as its value. Each JSON object must belong to one of the following types: 
	<ul>
		<li>SortDocID</li>
		<li>SortScore (which is the default if none is specified)</li>
		<li>SortGeoDistance</li> 
		<li>SortField</li>
	</ul>
</p>
<p align="justify">DocValues are relevant only when any of the JSON objects in the "sort" field are of type SortGeoDistance or SortField. This means that if you expect queries on a field F, where the queries either do not specify a value for the "sort" field or provide a JSON object of type SortDocID or SortScore, enabling docValues will not improve sorting operations, and as a result, query latency will remain unchanged. It's worth noting that the default sorting object, SortScore, does not require docValues to be enabled for any of the field mappings. Therefore, a search request without a sorting operation will not utilize docValues at all.</p>
<div style="overflow-x: auto;">
<table>
	<tr>
		<th>No Sort Objects</th>
		<th>SortDocID</th>
		<th>SortScore</th>
		<th>SortField</th>
		<th>SortGeoDistance</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field":"dolor"
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field":"sit_amet"
  },
  "sort":[
    {
     "by":"id",
     "desc":true
    }
    ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field":"sit_amet"
  },
  "sort":[
    {
     "by":"score",
    }
    ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field":"sit_amet"
  },
  "sort":[
    {
     "by":"field",
     "field":"dolor",
     "type":"auto",
     "mode":"min",
     "missing":"last"
    }
    ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "dolor"
  },
  "sort": [
    {
      "by": "geo_distance",
      "field": "sit_amet",
      "location": [
        123.223,
        34.33
      ],
      "unit": "km"
    }
  ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
	</tr>
	<tr align="center">
		<td>No DocValues used</td>
		<td>No DocValues used</td>
		<td>No DocValues used</td>
		<td>DocValues used for field "dolor". Field Mapping for "dolor" may enable docValues.</td>
		<td>DocValues used, for field "sit_amet". 
Field Mapping for "sit_amet" may enable docValues.</td>
	</tr>
</table>
</div>
<p align="justify">Now, let's consider faceting. The search request object also includes another field called "facets," where you can specify a collection of facet requests, with each request being associated with a unique name. Each of these facet requests can fall into one of three types:
<ul>
	<li>Date range</li> 
	<li>Numeric range</li>
	<li>Term facet</li> 
</ul>
Enabling docValues for the fields associated with such facet requests might provide benefits in this context.</p>
<div style="overflow-x: auto;">
<table>
	<tr>
		<th>No Facet Request</th>
		<th>Date Range Facet</th>
		<th>Numeric Range Facet</th>
		<th>Term Facet</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "dolor"
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "sit_amet"
  },
  "facet": {
    "facetA": {
      "size": 1,
      "field": "dolor",
      "date_ranges": [
        {
          "name": "lorem",
          "start": "20/August/2001",
          "end": "22/August/2002",
          "datetime_parser": "custDT"
        }
      ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "sit_amet"
  },
  "facet": {
    "facetA": {
      "size": 1,
      "field": "dolor",
      "numeric_ranges":[
          { 
            "name":"lorem",
            "min":22,
            "max":34
          }
        ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "sit_amet"
  },
  "facet": {
    "facetA": {
      "size": 1,
      "field": "dolor"
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
	</tr>
	<tr align="center">
		<td>No DocValues used</td>
		<td colspan="3">DocValues used for field "dolor". Field Mapping for "dolor" may enable docValues.</td>	
  </tr>
</table>
</div>

<p align="justify">In summary, when a search request is received by the Bleve index, it extracts all the fields from the sort objects and facet objects. To potentially benefit from docValues, you should consider enabling docValues for the fields mentioned in SortField and SortGeoDistance sort objects, as well as the fields associated with all the facet objects. By doing so, you can optimize sorting and faceting operations in your search queries.</p>

<div style="overflow-x: auto;">
<table>
	<tr>
		<th>Combo A</th>
		<th>Combo B</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "sit_amet"
  },
  "facet": {
    "facetA": {
      "size": 1,
      "field": "dolor",
      "date_ranges": [
        {
          "name": "lorem",
          "start": "20/August/2001",
          "end": "22/August/2002",
          "datetime_parser": "custDT"
        }
      ]
    }
  },
  "sort":[
    {
     "by":"field",
     "field":"lorem",
     "type":"auto",
     "mode":"min",
     "missing":"last"
    }
    ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "lorem ipsum",
    "field": "sit_amet"
  },
  "facet": {
    "facetA": {
      "size": 1,
      "field": "dolor",
      "numeric_ranges":[
          { 
            "name":"lorem",
            "min":22,
            "max":34
          }
        ]
    }
  },
  "sort": [
    {
      "by": "geo_distance",
      "field": "ipsum",
      "location": [
        123.223,
        34.33
      ],
      "unit": "km"
    }
  ],
  "size": 10,
  "from": 0
}
			</pre>
		</td>
	</tr>
	<tr align="center">
		<td>DocValues used for field "dolor" and "lorem". Field Mapping for "dolor" and "lorem" may enable docValues.</td>
		<td>DocValues used for field "dolor" and "ipsum". Field Mapping for "dolor" and "ipsum" may enable docValues.</td>
	</tr>
</table>
</div>

<h3>Empirical Analysis</h3>

<p align="justify">To evaluate our hypothesis, I've set up a sample dataset on my personal computer and I've created two Bleve indexes: one with docvalues enabled for three fields (<code>dummyDate</code>, <code>dummyNumber</code>, and <code>dummyTerm</code>), and another where I've disabled docValues for the same three fields. These field mappings were incorporated into the Default Mapping. It's important to mention that for both indexes, DocValues for dynamic fields were enabled, as the default mapping is dynamic.</p>

<p align="justify">The values for <code>dummyDate</code> and <code>dummyNumber</code> were configured to increase monotonically, with <code>dummyDate</code> representing a date value and `dummyNumber` representing a numeric value. This setup was intentional to ensure that facet aggregation would consistently result in cache hits and misses, providing a useful testing scenario.</p>

<div style="overflow-x: auto;">
<table>
	<tr>
		<th>Index A</th>
		<th>Index B</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
   "default_mapping": {
    "dynamic": true,
    "enabled": true,
    "properties": {
     "dummyNumber": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyNumber",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": true
       }
      ]
     },
     "dummyTerm": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyTerm",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": true
       }
      ]
     },
     "dummyDate": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyDate",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": true
       }
      ]
     }
    }
   }
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
   "default_mapping": {
    "dynamic": true,
    "enabled": true,
    "properties": {
     "dummyNumber": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyNumber",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": false
       }
      ]
     },
     "dummyTerm": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyTerm",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": false
       }
      ]
     },
     "dummyDate": {
      "enabled": true,
      "dynamic": false,
      "fields": [
       {
        "name": "dummyDate",
        "type": "text",
        "store": false,
        "index": true,
        "include_term_vectors": false,
        "include_in_all": false,
        "docvalues": false
       }
      ]
     }
    }
   }
			</pre>
		</td>
	</tr>
	<tr align="center">
		<td>Docvalues enabled across all three field mappings</td>
		<td>Docvalues disabled across all three field mappings</td>
	</tr>
</table>
</div>

Document Format used for the test scenario:

<div style="overflow-x: auto;">
<table>
	<tr>
		<th>Document 1</th>
		<th>Document 2</th>
		<th>... Document i</th>
		<th>Document 5000</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
	"dummyTerm":"Term",
	"dummyDate":"2000-01-01T00:00:00,
	"dummyNumber:1
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
	"dummyTerm":"Term",
	"dummyDate":"2000-01-01T01:00:00,
	"dummyNumber:2
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
	"dummyTerm":"Term",
	"dummyDate":"2000-01-01T01:00:00"+(i hours),
	"dummyNumber:i
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
	"dummyTerm":"Term",
	"dummyDate":2000-01-01T01:00:00 + (5000 hours),
	"dummyNumber:5000
}
			</pre>
		</td>
</table>
</div>

<p align="justify">Now I ran the following set of search requests across both the indexes, while increasing the number of documents indexed from 2000 to 4000.</p>

<div style="overflow-x: auto;">
<table>
	<tr>
		<th>Request 1</th>
		<th>Request 2</th>
		<th>... Request i</th>
		<th>Request 1000</th>
	</tr>
	<tr>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "term",
    "field":"dummyTerm"
  },
  "facets":{
    "myDate":{
      "field":"dummyDate",
      "size":100000,
      "date_ranges":[
        {
          "start":"2000-01-01T00:00:00",
          "end":"2000-01-01T01:00:00"
        }
      ]
    },
    "myNum":{
      "field":"dummyNumber",
      "size":100000,
      "numeric_ranges":[
        {
          "min": 1000,
          "max": 1001
        }
      ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "term",
    "field":"dummyTerm"
  },
  "facets":{
    "myDate":{
      "field":"dummyDate",
      "size":100000,
      "date_ranges":[
        {
          "start":"2000-01-01T01:00:00",
          "end":"2000-01-01T02:00:00"
        }
      ]
    },
    "myNum":{
      "field":"dummyNumber",
      "size":100000,
      "numeric_ranges":[
        {
          "min": 999,
          "max": 1000
        }
      ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "term",
    "field":"dummyTerm"
  },
  "facets":{
    "myDate":{
      "field":"dummyDate",
      "size":100000,
      "date_ranges":[
        {
          "start":"2000-01-01T00:00:00" + i hour
          "end":"2000-01-01T00:00:00" + (i+1) hour
        }
      ]
    },
    "myNum":{
      "field":"dummyNumber",
      "size":100000,
      "numeric_ranges":[
        {
          "min": 1000-i,
          "max": 1000-i+1
        }
      ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
		<td style="vertical-align: top; width: 20%;">
			<pre>
{
  "explain": true,
  "fields": [
    "*"
  ],
  "highlight": {},
  "query": {
    "match": "term",
    "field":"dummyTerm"
  },
  "facets":{
    "myDate":{
      "field":"dummyDate",
      "size":100000,
      "date_ranges":[
        {
          "start":"2000-01-01T01:00:00" + 1000 hour,
          "end":"2000-01-01T02:00:00" + 1001 hour
        }
      ]
    },
    "myNum":{
      "field":"dummyNumber",
      "size":100000,
      "numeric_ranges":[
        {
          "min": 0,
          "max": 1
        }
      ]
    }
  },
  "size": 10,
  "from": 0
}
			</pre>
		</td>
</table>
</div>


<div style="overflow-x: auto;">
<table>
	<tr>
		<th>Bleve index size growth with increase in indexed documents</th>
		<th>Total query time for 1000 queries with increase in number of indexed documents</th>
	</tr>
		<td>
<img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAoAAAAHgCAYAAAA10dzkAAAAOXRFWHRTb2Z0d2FyZQBNYXRwbG90bGliIHZlcnNpb24zLjcuMSwgaHR0cHM6Ly9tYXRwbG90bGliLm9yZy/bCgiHAAAACXBIWXMAAA9hAAAPYQGoP6dpAAB4M0lEQVR4nO3deZyN5f/H8deZfRgz9rHve/YoS3aRLUuLrxTiSxghJZElkqFStBGJVBIydrLN2Ckia2NfqrFUzJjBbOf6/XG+zs9kG9vcM+e8n4/HeZhzb+dzz3Gct+u6r+u2GWMMIiIiIuI2PKwuQERERETSlgKgiIiIiJtRABQRERFxMwqAIiIiIm5GAVBERETEzSgAioiIiLgZBUARERERN6MAKCIiIuJmFABFRERE3IwCoIiIiIibUQAUERERcTMKgCIiIiJuRgFQRERExM0oAIqIiIi4GQVAERERETejACgiIiLiZhQARURERNyMAqCIiIiIm1EAFBEREXEzCoAiIiIibkYBUERERMTNKACKiIiIuBkFQBERERE3owAoIiIi4mYUAEVERETcjAKgiIiIiJtRABQRERFxMwqAIiIiIm5GAVBERETEzSgAioiIiLgZBUARERERN6MAKCIiIuJmFABFRERE3IwCoIiIiIibUQAUERERcTMKgCIiIiJuRgFQRERExM0oAIqIiIi4GQVAERERETejACgiIiLiZhQARURERNyMAqCIiIiIm1EAvE/Wr19Pq1atyJcvHzabjQULFtzxMYwxvP/++5QqVQpfX1/y58/PO++8c/+LFREREbfmZXUBriIuLo5KlSrRtWtX2rVrd1fH6NevHytXruT999+nQoUK/PPPP/zzzz/3uVIRERFxdzZjjLG6CFdjs9kICwujTZs2zmXx8fG8+eabfPfdd1y4cIHy5cszbtw46tevD8CBAweoWLEie/fupXTp0tYULiIiIm5BXcBppE+fPmzZsoXZs2eze/dunnnmGZ544gkOHToEwOLFiylWrBhLliyhaNGiFClShP/+979qARQREZH7TgEwDZw8eZLp06czd+5c6tSpQ/HixXnttdd47LHHmD59OgBHjx7lxIkTzJ07l5kzZzJjxgx27NjB008/bXH1IiIi4mp0DWAa2LNnD8nJyZQqVSrF8vj4eHLkyAGA3W4nPj6emTNnOrebNm0aDz/8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atWqxZEjR1IcZ+HChVStWhU/Pz+KFSvGyJEjSUpKSnHcL774grZt25IpUyZKlizJokWLAEe3fYMGDQDIli0bNpuNLl26ADBv3jwqVKiAv78/OXLkoHHjxsTFxd3wdygi6Y8CoIgb8fT0ZMyYMXz88cf8/vvv93SstWvX8ueff7J+/Xo++OADRowYQcuWLcmWLRvbtm2jZ8+evPTSS9e9zsCBA3n11VfZuXMnNWvWpFWrVvz9998AXLhwgYYNG1KlShW2b9/OihUrOHPmDM8++2yKY3z11Vf4+PiwadMmJk+efMP6Jk6cyPjx43n//ffZvXs3TZs25cknn3QGs6ioKB566CFeffVVoqKieO211254nIEDB7Ju3ToWLlzIypUriYiI4JdffkmxTZcuXdi+fTuLFi1iy5YtGGNo3rw5iYmJAOzatYtGjRpRrlw5tmzZwsaNG2nVqhXJycl39Dt/++236dSpE7t27aJMmTI899xzvPTSSwwePJjt27djjKFPnz7O7Tds2ECnTp3o168f+/fv5/PPP2fGjBm88847KY47cuRInn32WXbv3k3z5s3p2LEj//zzDwULFuSHH34AIDIykqioKCZOnEhUVBQdOnSga9euHDhwgIiICNq1a4duLS+SgRgRcQudO3c2rVu3NsYYU6NGDdO1a1djjDFhYWHm2n8KRowYYSpVqpRi3w8//NAULlw4xbEKFy5skpOTnctKly5t6tSp43yelJRkMmfObL777jtjjDHHjh0zgBk7dqxzm8TERFOgQAEzbtw4Y4wxb7/9tmnSpEmK1z516pQBTGRkpDHGmHr16pkqVarc9nzz5ctn3nnnnRTLqlevbnr37u18XqlSJTNixIibHuPixYvGx8fHzJkzx7ns77//Nv7+/qZfv37GGGMOHjxoALNp0ybnNn/99Zfx9/d37tehQwdTu3btm75O4cKFzYcffphi2b9rA8zQoUOdz7ds2WIAM23aNOey7777zvj5+TmfN2rUyIwZMybFcb/++muTN2/emx43NjbWAGb58uXGGGPCw8MNYM6fP+/cZseOHQYwx48fv+k5iUj6phZAETc0btw4vvrqKw4cOHDXx3jooYfw8Pj/f0KCg4OpUKGC87mnpyc5cuTg7NmzKfarWbOm82cvLy+qVavmrOPXX38lPDycgIAA56NMmTIAKbo2H3744VvWFhMTw59//knt2rVTLK9du/YdnfORI0dISEjg0UcfdS7Lnj07pUuXdj4/cOAAXl5eKbbJkSMHpUuXdr7W1RbAe1WxYkXnz8HBwQApfufBwcFcuXKFmJgYwPH7HDVqVIrfZ/fu3YmKiuLSpUs3PG7mzJkJDAy87n27VqVKlWjUqBEVKlTgmWeeYerUqS51XamIO/CyugARSXt169aladOmDB482HlN11UeHh7XdeVd7cq8lre3d4rnNpvthsvsdnuq64qNjaVVq1aMGzfuunV58+Z1/pw5c+ZUHzM98Pf3v+X6u/mdXx25faNlV3/nsbGxjBw5knbt2l13LD8/vxse9+pxbvW+eXp6smrVKjZv3szKlSv5+OOPefPNN9m2bRtFixa96X4ikn6oBVDETY0dO5bFixezZcuWFMtz5crF6dOnUwSS+zl339atW50/JyUlsWPHDsqWLQtA1apV2bdvH0WKFKFEiRIpHncS+gIDA8mXLx+bNm1KsXzTpk2UK1cu1ccpXrw43t7ebNu2zbns/PnzHDx40Pm8bNmyJCUlpdjm77//JjIy0vlaFStWvOU0M7ly5SIqKsr5PCYmhmPHjqW6zpupWrUqkZGR1/0uS5QokaL19lZ8fHwArrte0WazUbt2bUaOHMnOnTvx8fEhLCzsnmsWkbShACjipipUqEDHjh356KOPUiyvX78+586d49133+XIkSN8+umnLF++/L697qeffkpYWBi//fYbISEhnD9/nq5duwIQEhLCP//8Q4cOHfj55585cuQIP/74Iy+++OIdD5gYOHAg48aN4/vvvycyMpI33niDXbt20a9fv1QfIyAggG7dujFw4EDWrl3L3r176dKlS4rwVLJkSVq3bk337t3ZuHEjv/76K88//zz58+endevWAAwePJiff/6Z3r17s3v3bn777TcmTZrkHN3csGFDvv76azZs2MCePXvo3Lkznp6ed3S+NzJ8+HBmzpzJyJEj2bdvHwcOHGD27NkMHTo01ccoXLgwNpuNJUuWcO7cOWJjY9m2bRtjxoxh+/btnDx5kvnz53Pu3DlnkBeR9E8BUMSNjRo16rquvrJly/LZZ5/x6aefUqlSJX766aebjpC9G2PHjmXs2LFUqlSJjRs3smjRInLmzAngbLVLTk6mSZMmVKhQgf79+5M1a9ZUt1hd1bdvXwYMGMCrr75KhQoVWLFiBYsWLaJkyZJ3dJz33nuPOnXq0KpVKxo3bsxjjz123TWI06dP5+GHH6Zly5bUrFkTYwzLli1zdq2WKlWKlStX8uuvv/LII49Qs2ZNFi5ciJeX4yqcwYMHU69ePVq2bEmLFi1o06YNxYsXv6M6b6Rp06YsWbKElStXUr16dWrUqMGHH35I4cKFU32M/PnzM3LkSN544w2Cg4Pp06cPgYGBrF+/nubNm1OqVCmGDh3K+PHjadas2T3XLCJpw2b+feGJiIiIiLg0tQCKiIiIuBkFQBERERE3owAoIiIi4mYUAEVERETcjAKgiIiIiJtRABQRERFxMwqAIiIiIm5GAVBERETEzSgAioiIiLgZBUARERERN6MAKCIiIuJmFABFRERE3Mz/AXie0cZH5fOPAAAAAElFTkSuQmCC" alt="Image 1" />
</td>
		<td>
			<img src="data:image/png;base64,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H3ENG8HTT5P+xA7Ok57xpb4i6NQfBLara3d1kgBcLgBOmDCBTJkyUb9+/btuV6FCBZYtWxZv2ZIlS6hQocKjK84YuHIl8W/30c+nWrVqrFixwvl4xYoVVK1alSpVqjiXX7t2jfXr1zsD4D9VrFiRESNG4O/v7+yR/cYbbzjXDx8+nLJly7J161a6detG165dnUHrypUr1K5dm4CAADZu3MjPP//M0qVLeeWVV+65/hkzZpA9e3Zny15YWNg9P/emnj17Yoxh9mzrAucTJ05Qr149Hn/8cf744w9GjhzJuHHj+PDDD53P6du3L0OHDuX9999n165dTJkyxXmJQb169cicOTMTJ06M9zoTJkygWbNmpEuXjitXrvDaa6+xadMmli1bhoeHB02bNiUuLu6+679p1apVtGvXjp49e7Jr1y5Gjx7NxIkTncF0+vTpfP7554wePZp9+/Yxa9YsihUr9sCvJyIu4NIleP11TJEieMz9lWhSMIKejH97Hy9sfoWUab3srlASinEhsbGxJmfOnOatt966Zd3zzz9v3n77befjNWvWmBQpUphPP/3U7N692/Tr1894eXmZ7du33/PrhYeHG8CEh4ffsu7atWtm165d5tq1a38vjIw0xopjiXuLjLznYxo7dqxJlSqViY6ONhERESZFihTmzJkzZsqUKaZy5crGGGOWLVtmAHPkyBFjjDErVqwwgLl48aIxxpgJEyaYtGnT3rLv4OBg07ZtW+fjuLg4kylTJjNy5EhjjDFjxowxAQEBJvIf9c6bN894eHiYU6dOGWOMqVKliunZs2e8/TZu3Ni0b98+3ut8/vnn/3msd9suc+bMpmvXrsYYY9555x1ToEABExcX51z/zTffmNSpU5vY2FgTERFhfHx8zNixY+/4Wm+//bbJnTu3cx/79+83DofDLF269Lbbnz171gDO38dDhw4ZwGzdutUYc/uf8cyZM80/P9I1atQwgwcPjrfNDz/8YLJkyWKMMWb48OEmf/785saNG3es+59u+zstIq4hOtqYb781JkMG53fHr9Q3JX13m+nT7S4u4d3t+9tduFQL4NKlSzl69CgdO3a8Zd3Ro0fjtQZVrFiRKVOmMGbMGEqUKMEvv/zCrFmzKFq0aGKWnORUrVqVK1eusHHjRlatWkX+/PkJDAykSpUqzusAQ0JCyJMnDzlz5rzv/Rcv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oXv37ly8eJGOHTsC0L17dy5cuECrVq3YuHEjBw4cYNGiRbzwwgv33WGiT58+DBs2jJ9++om9e/fy9ttvExoaSs+ePe95H6lTp6ZTp0706dOH5cuXs2PHDjp06BAvPOXLl4/GjRvTuXNnVq9ezR9//EHbtm3Jli0bjRs3BqBv375s3LiRbt26sW3bNvbs2cPIkSOdvZurV6/ODz/8wKpVq9i+fTvt27fH09Pzvo73dj744AMmTZrEgAED2LlzJ7t372bq1Km8995797yP4OBgHA4Hc+fO5ezZs0RGRrJ+/XoGDx7Mpk2bOHr0KDNmzODs2bPOIC8iSZ8CoIgbGzhw4C2n+goVKsS3337LN998Q4kSJdiwYcMde8g+iKFDhzJ06FBKlCjB6tWrmTNnDhkzZgRwttrFxsZSq1YtihUrRq9evUiXLt09t1jd1KNHD1577TVef/11ihUrxsKFC5kzZw758uW7r/188sknPPXUUzRs2JCaNWvy5JNP3nIN4oQJEyhTpgwNGjSgQoUKGGOYP3++89Rq/vz5Wbx4MX/88QflypWjQoUKzJ49mxQprKtw+vbtS5UqVWjQoAH169enSZMmPPbYY/dV5+3Url2buXPnsnjxYh5//HHKly/P559/TnBw8D3vI1u2bAwYMIC3336bzJkz88orr+Dv789vv/1GvXr1yJ8/P++99x7Dhw+nbt26D12ziCQOh/n3hSciIiIi4tLUAigiIiLiZhQARURERNyMAqCIiIiIm1EAFBEREXEzCoAiIiIibkYBUERERMTNKACKiIiIuBkFQBERERE3owAoIiIi4mYUAEVERETcjAKgiIiIiJtRABQRERFxM/8HvOd3yw96K2AAAAAASUVORK5CYII=" alt="Image 2" />
		</td>
	</tr>
</table>
</div>

<div style="overflow-x: auto;">
<table>
	<tr>
		<th style="width:50%">Average increase in index size in bytes</th>
		<th style="width:50%">Average reduction in time taken to perform 1000 queries in milliseconds</th>
	</tr>
	<tr>
		<td align="center"><code>7762.47</code></td>
		<td align="center"><code>27.034</code></td>
	</tr>
</table>
Even at this small scale, with a small document size and a very limited number of indexed documents, we still observe a noticeable tradeoff. With just a slight increase in the index size (an average of 7KB) we obtain a 20ms reduction in the total execution time, on average, for only 1000 queries.

<h3>Technical Information</h3>

<p align="justify">When a search request involves facet or sorting operations on a field F, these operations occur after the main search query is executed. For instance, if the main query yields a result of 200 documents, the sorting and faceting processes will be applied to these 200 documents. However, the main query result only provides a set of document IDs, not the actual document contents.</p>

<p align="justify">Here's where docValues become essential. If the field mapping for F is docValue enabled, the system can directly access the values for the field from the stored docValue part in the index file. This means that for each document ID returned in the search result, the field values are readily available.</p>

<p align="justify">However, if docValues are not enabled for field F, the system must take a different approach. It needs to "fetch the document" from the index file, read the value for field F, and cache this field-document pair in memory for further processing. The issue becomes apparent in the latter scenario. By not enabling docValues for field F, you essentially retrieve all the documents in the search result (at the worst case), which can be a substantial amount of data. Moreover, you have to cache this information in memory, leading to increased memory usage. As a result, query latency significantly suffers because you're essentially fetching and processing all documents, which can be both time-consuming and resource-intensive. Enabling docValues for the relevant fields is, therefore, a crucial optimization to enhance query performance and reduce memory overhead in such situations.</p>
