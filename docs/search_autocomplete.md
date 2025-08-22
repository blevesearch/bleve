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

### 3.3 N-gram Method

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

### 3.4 Edge N-gram Method (preferred for autocomplete)

```go
// analysis/token/edgengram/edgengram.go
func (s *EdgeNgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
    rv := make(analysis.TokenStream, 0, len(input))
    
    for _, token := range input {
        runeCount := utf8.RuneCount(token.Term)
        runes := bytes.Runes(token.Term)
        // ..builds tokens based from either end, specified in the input
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

## 5. Step-by-Step Implementation: Building Search Autocomplete

Now let's see how to implement this in practice using our exact configuration. We'll build a complete autocomplete system step by step.

### Step 1: Create Custom Edge N-gram Token Filter

First, we need to create a custom token filter for edge n-grams. Here's how it looks in our configuration:

```json
{
  "token_filters": {
    "Engram": {
      "type": "edge_ngram",
      "min": 2,
      "max": 4,
      "back": "false"
    }
  }
}
```

**What each setting does:**
- `"type": "edge_ngram"` - Tells Bleve to use the edge n-gram filter
- `"min": 2` - Start creating tokens from 2 characters ("ja", "sc", etc.)
- `"max": 4` - Stop at 4 characters ("java", "scri", etc.)
- `"back": "false"` - Create tokens from the front (beginning) of words

![Edge N-gram Token Filter Configuration](/docs/custom_token_filter.png "Custom token filter setup showing edge_ngram configuration")

### Step 2: Build the Complete Analyzer Pipeline

Next, we create an analyzer that uses our custom token filter along with other helpful filters:

```json
{
  "analyzers": {
    "search_autocomplete_feature": {
      "type": "custom",
      "tokenizer": "unicode",
      "char_filters": [
        "zero_width_spaces"
      ],
      "token_filters": [
        "Engram",
        "to_lower", 
        "stop_en"
      ]
    }
  }
}
```

**The pipeline works like this:**

1. **Input Text**: "Schaumbergfest Event"

2. **Tokenizer** (`unicode`): Splits into words
   ```
   ["Schaumbergfest", "Event"]
   ```

3. **Character Filter** (`zero_width_spaces`): Removes invisible characters
   ```
   ["Schaumbergfest", "Event"] (cleaned)
   ```

4. **Token Filter 1** (`Engram`): Creates edge n-grams (2-4 chars)
   ```
   ["Sc", "Sch", "Scha", "Ev", "Eve", "Even"]
   ```

5. **Token Filter 2** (`to_lower`): Makes everything lowercase
   ```
   ["sc", "sch", "scha", "ev", "eve", "even"]
   ```

6. **Token Filter 3** (`stop_en`): Removes common words (none in this case)
   ```
   ["sc", "sch", "scha", "ev", "eve", "even"] (final tokens)
   ```

### Step 3: Configure Field Mapping

Now we tell Bleve which fields to apply our autocomplete analyzer to:

```json
{
  "default_mapping": {
    "properties": {
      "name": {
        "fields": [
          {
            "name": "name",
            "type": "text",
            "analyzer": "search_autocomplete_feature",
            "store": true,
            "index": true,
            "include_in_all": true,
            "include_term_vectors": true,
            "docvalues": true
          }
        ]
            }
        }
    }
}
```

**Field configuration explained:**
- `"analyzer": "search_autocomplete_feature"` - Use our custom analyzer
- `"store": true` - Keep original text for display
- `"index": true` - Make it searchable
- `"include_in_all": true` - Include in default search field

![Index Mapping Configuration](/docs/name_filed_searchable_search_autocomplete_analyzer.png "Index mapping showing how the name field is configured with the custom analyzer")

### Step 4: How It Works in Real Search

When someone searches for "sc", here's what happens:

**Index contains these tokens:**
```
"sc" → [document1: "Schaumbergfest", document2: "Script", ...]
"sch" → [document1: "Schaumbergfest", ...]  
"scha" → [document1: "Schaumbergfest", ...]
```

**User types "sc":**
1. Query: `name:sc`
2. Bleve looks up exact term "sc" in the index
3. Finds document with "Schaumbergfest" 
4. Returns suggestion instantly

![Search Results](/docs/index_search_using_prefix.png "Search results showing 'Schaumbergfest' highlighted when searching for 'sc'")

