# Edge N-gram Autocomplete in Bleve

Search autocomplete is a feature which we see in search boxes when suggestions appear while we type.
So when we type `jav`, we see suggestions like: `java` `javascript` `javascript programming` etc.
How this is helpful, it actually saves time for user to find what they are looking for!

![Alt Text](/docs/search_autocomplete.png "search autocomplete suggestion")

## 2. How Does It Work?

Autocomplete generally works in three steps:

1. **Index Time**: Breaking text into searchable pieces (tokens)
2. **Query Time**: Matching user input against indexed tokens  
3. **Results**: Returning relevant suggestions quickly

But before we jump into the flow, let's understand different methods to achieve this and why edge n-grams are the most efficient approach.

## 3. Different Tokenization Methods

There are several tokenization approaches, each with its own strengths and weaknesses:

### 3.1 Single Token Method

```go
// analysis/tokenizer/single/single.go
func (t *SingleTokenTokenizer) Tokenize(input []byte) analysis.TokenStream {
    return analysis.TokenStream{
        &analysis.Token{
            Term:     input,    // Here entire input as one token
            Position: 1,
            Start:    0,
            End:      len(input),
            Type:     analysis.AlphaNumeric,
        },
    }
}
```

**How it works**: Treats the entire input as a single token.

**Example**: "JavaScript Programming" → [`"JavaScript Programming"`]

**Pros**: 
- Simple and fast
- Perfect for exact phrase matching
- Minimal index size

**Cons**: 
- No autocomplete support (can't match partial text)
- Not flexible for search

**Use case**: Keyword fields, IDs, exact phrase matching

### 3.2 Whitespace Token Method

```go
// analysis/tokenizer/whitespace/whitespace.go  
func TokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
    return character.NewCharacterTokenizer(notSpace), nil
}

func notSpace(r rune) bool {
    return !unicode.IsSpace(r)  // Split on whitespace
}
```

**How it works**: Splits text on whitespace characters.

**Example**: "JavaScript Programming" → [`"JavaScript"`, `"Programming"`]

**Pros**: 
- Simple word-based tokenization
- Works well for basic prefix search

**Cons**: 
- Only matches from word beginnings
- No support for partial word matching
- Limited autocomplete capabilities

**Use case**: Basic search, word-level indexing

### 3.3 Regular Expression Method

```go
// analysis/tokenizer/regexp/regexp.go
func (rt *RegexpTokenizer) Tokenize(input []byte) analysis.TokenStream {
    matches := rt.r.FindAllIndex(input, -1)  // Find all regex matches
    rv := make(analysis.TokenStream, 0, len(matches))
    
    for i, match := range matches {
        matchBytes := input[match[0]:match[1]]
        if match[1]-match[0] > 0 {
            token := analysis.Token{
                Term:     matchBytes,
                Position: i + 1,
                // ... other fields
            }
            rv = append(rv, &token)
        }
    }
    return rv
}
```

**How it works**: Uses regular expressions to define token boundaries.

**Example**: With `\w+` regex, "JavaScript-Programming!" → [`"JavaScript"`, `"Programming"`]

**Pros**: 
- Flexible and customizable
- Can handle complex tokenization rules
- Good for specialized text formats

**Cons**: 
- More complex to configure
- Still limited to prefix matching for autocomplete
- Performance depends on regex complexity

**Use case**: Complex text parsing, specialized formats

### 3.4 N-gram Method

```go
// analysis/token/ngram/ngram.go
func (s *NgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
    rv := make(analysis.TokenStream, 0, len(input))
    
    for _, token := range input {
        runeCount := utf8.RuneCount(token.Term)
        runes := bytes.Runes(token.Term)
        
        // ..generate all possible n-grams
    }
    return rv
}
```

**How it works**: Creates ALL possible substrings of specified lengths.

**Example**: "java" with 2-3 grams → [`"ja"`, `"av"`, `"va"`, `"jav"`, `"ava"`]

**Pros**: 
- Supports partial matching anywhere in the text
- Coverage of all possible substrings

**Cons**: 
- **MASSIVE index size** (exponential growth)
- Brings more noise as irrelevant matches (e.g., "av" matching "java")
- Poor performance for autocomplete
- High memory usage

**Use case**: Full-text substring search (not ideal for autocomplete)

### 3.5 Edge N-gram Method (preferred for autocomplete)

```go
// analysis/token/edgengram/edgengram.go
func (s *EdgeNgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
    rv := make(analysis.TokenStream, 0, len(input))
    
    for _, token := range input {
        runeCount := utf8.RuneCount(token.Term)
        runes := bytes.Runes(token.Term)
        // ..builds tokens based form either end, specified in the input
    }
    return rv
}
```

**How it works**: Creates substrings only from the beginning (or end) of each word.

**Example**: "javascript" with front edge n-grams (1-5) → [`"j"`, `"ja"`, `"jav"`, `"java"`, `"javas"`]

**Pros**: 
- Perfect for autocomplete (matches prefixes naturally)
- Efficient index size (linear growth vs exponential)  
- Fast queries (direct term matching, no complex processing)
- Intuitive results (matches what users expect)
- Highly scalable for large datasets

**Cons**: 
- Only supports prefix matching (but that's preferred for autocomplete!)
- Slightly larger index than basic tokenization

**Use case**: Search autocomplete, prefix-based search

## 4. Why Edge N-grams Are Most Efficient for Autocomplete

Let's see what happens when a user types "java" with edge_ngram tokenizer:

```
Index contains: ["j", "ja", "jav", "java", "javas", "javasc", "javascr", ...]
User types: "java"  
Query: ExactTermQuery("java")
Process: Direct hash table lookup for term "java"
Result: Instant match, then retrieve documents containing this term
```

**Key advantages of Edge N-gram approach:**

1. **O(1) lookup**: Direct term matching
2. **No query-time processing**: Terms are pre-computed at index time  
3. **Better caching**: Exact term queries cache better than prefix queries
4. **Consistent performance**: Query time doesn't increase with index size

## 5. On low level implementaion sample:

How edge n-gram would look like at low level:

### Step 1: Setup Analyzer

```go
func createAutocompleteMapping() *mapping.IndexMappingImpl {
    indexMapping := bleve.NewIndexMapping()
    
    // Add edge n-gram analyzer
    err := indexMapping.AddCustomAnalyzer("autocomplete", map[string]interface{}{
        "type": "custom",
        "tokenizer": "unicode",
        "token_filters": []interface{}{
            "to_lower",
            map[string]interface{}{
                "type": "edge_ngram",
                "side": "front",
                "min":  1,
                "max":  15,
            },
        },
    })
    
    return indexMapping
}
```

### Step 2: Tokenization Process

When we index "JavaScript Programming":

```
1. Original: "JavaScript Programming"
2. Tokenize: ["JavaScript", "Programming"]  
3. Lowercase: ["javascript", "programming"]
4. Edge n-grams: ["j", "ja", "jav", "java", "javas"] + ["p", "pr", "pro", "prog"]
```

### Step 3: Simple Search Function

```go
func autocompleteSearch(index bleve.Index, userInput string) ([]string, error) {
    // Use exact term query (faster than prefix queries)
    query := bleve.NewTermQuery(strings.ToLower(userInput))
    
    searchRequest := bleve.NewSearchRequest(query)
    searchRequest.Size = 10
    searchRequest.Fields = []string{"title"}
    
    searchResult, err := index.Search(searchRequest)
    if err != nil {
        return nil, err
    }
    
    // Extract suggestions
    var suggestions []string
    for _, hit := range searchResult.Hits {
        if title, exists := hit.Fields["title"]; exists {
            suggestions = append(suggestions, title.(string))
        }
    }
    
    return suggestions, nil
}
```

### Step 4: JSON Configuration Example

Here's what the edge n-gram configuration looks like in JSON format:

```json
{
  "analysis": {
    "analyzers": {
      "autocomplete": {
        "type": "custom",
        "tokenizer": "standard",
        "filters": [
          "lowercase",
          "edge_ngram_filter"
        ]
      }
    },
    "filters": {
      "edge_ngram_filter": {
        "type": "edge_ngram",
        "min_gram": 1,
        "max_gram": 15,
        "side": "front"
      }
    }
  }
}
```

**What each part does:**
- `"tokenizer": "standard"` - Splits text into words
- `"lowercase"` - Makes everything lowercase so "Java" and "java" match
- `"edge_ngram_filter"` - Creates prefixes from each word
- `"min_gram": 1` - Shortest prefix is 1 character ("j")
- `"max_gram": 15` - Longest prefix is 15 characters ("javascriptprogr")
- `"side": "front"` - Create prefixes from the beginning, not the end

### Step 5: Complete Mapping JSON

Here's a full index mapping that includes edge n-gram for autocomplete:

```json
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "analyzer": "standard",
        "fields": {
          "autocomplete": {
            "type": "text",
            "analyzer": "autocomplete",
            "search_analyzer": "standard"
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "standard"
      },
      "category": {
        "type": "keyword"
      }
    }
  }
}
```

**Why this mapping is smart:**
- `title` field uses standard analyzer for normal search
- `title.autocomplete` field uses edge n-gram analyzer for autocomplete
- `search_analyzer: standard` means we search with whole words, not edge n-grams
- This gives us the best of both worlds!

### Step 6: How autocomplete suggestion looks in real-time

As user types progressively:

- **"j"** → ["JavaScript: The Good Parts", "Java Complete Guide", "JavaFX Desktop Apps"]
- **"ja"** → ["JavaScript: The Good Parts", "Java Complete Guide", "JavaFX Desktop Apps"]  
- **"jav"** → ["JavaScript: The Good Parts", "JavaFX Desktop Apps"]
- **"java"** → ["JavaScript: The Good Parts", "JavaFX Desktop Apps"]

### Step 7: What happens behind the scenes

**During indexing:**
```
Document: "JavaScript Programming"
↓
Tokenize: ["JavaScript", "Programming"]
↓
Lowercase: ["javascript", "programming"] 
↓
Edge n-grams: ["j", "ja", "jav", "java", "javas", "javasc", "javascr", "javascript"]
              ["p", "pr", "pro", "prog", "progr", "program", "programm", "programmi", "programmin", "programming"]
```

**During search:**
```
User types: "jav"
↓
Search for exact term: "jav"
↓
Find documents containing token "jav"
↓
Return: Documents with "JavaScript" in title
```

## 6. Discussion: Why This Approach Works So Well

**The Problem with Other Methods:**
- **Prefix queries**: Must scan through ALL terms to find matches → slow
- **Full n-grams**: Create too many useless tokens like "vas", "ascr" → huge index
- **Simple tokenization**: Can't match partial words → limited autocomplete

**The Edge N-gram Solution:**
- **Pre-computes** all useful prefixes during indexing
- **Direct lookup** during search - no scanning needed
- **Only useful tokens** - prefixes people actually type
- **Scales perfectly** - search time stays constant even with millions of documents

**Real-world Benefits:**
- **Fast response**: Autocomplete shows up in < 50ms
- **Predictable performance**: Doesn't slow down as data grows  
- **User-friendly**: Matches exactly what people expect
- **Resource efficient**: Uses memory wisely compared to full n-grams

**Trade-offs to Consider:**
- **Index size**: 3-5x larger than basic tokenization
- **Memory usage**: More RAM needed for larger indexes
- **Prefix-only**: Can't match middle or end of words (but that's usually fine for autocomplete)

This is why major search platforms like Elasticsearch and modern applications use edge n-grams for autocomplete - it's the sweet spot between performance and functionality!

## 7. Best Practices & Production Tips

### Configuration Recommendations

**For Most Use Cases:**
```json
{
  "min_gram": 2,
  "max_gram": 15
}
```
- Start from 2 characters (avoids single-letter noise)
- Stop at 15 characters (covers most search terms)

**For Product Names/Brands:**
```json
{
  "min_gram": 1,
  "max_gram": 20  
}
```
- Include single characters for brand initials
- Longer max for product model numbers

### Memory Management

**Index Size Guidelines:**
- **Small dataset** (< 100K docs): Edge n-grams are perfect
- **Medium dataset** (100K - 1M docs): Monitor memory usage
- **Large dataset** (> 1M docs): Consider shorter max_gram or dedicated autocomplete fields

**Optimization Tips:**
1. **Use separate autocomplete fields** - don't apply edge n-grams to all text fields
2. **Limit field length** - truncate very long text before indexing
3. **Choose selective fields** - only apply to titles, names, key phrases

### Query Performance

**Do:**
```go
// Use exact term queries (fast)
query := bleve.NewTermQuery("javascript")
```

**Don't:**
```go
// Avoid prefix queries with edge n-grams (slow)
query := bleve.NewPrefixQuery("javascript")
```

### Real-world Implementation

**E-commerce Example:**
```json
{
  "product_name": {
    "type": "text",
    "fields": {
      "autocomplete": {
        "analyzer": "edge_ngram_analyzer",
        "min_gram": 2,
        "max_gram": 12
      }
    }
  }
}
```

**Content Search Example:**
```json
{
  "article_title": {
    "type": "text", 
    "fields": {
      "autocomplete": {
        "analyzer": "edge_ngram_analyzer",
        "min_gram": 3,
        "max_gram": 20
      }
    }
  }
}
```
