module SnowflakeWrapper

using DataFramesMeta, DataFrames, ODBC, CSV, IterTools, Arrow, Base.Iterators

INSERT_CHUNK_LIMIT = 16000
REGEX = r"({ \w+ })"

struct dbClass
    conn::ODBC.Connection
end

function initWrapper()
    env_keys = keys(ENV)
    if !("ACCOUNT" in env_keys) || !("DBUSER" in env_keys) || !("PASSWD" in env_keys)
        @warn "Missing value in env Either ACCOUNT , USER or PASSWD"
        return nothing
    end
    @debug "Opening connection from ENV settings"
    dbClass(ODBC.Connection("Driver=$(ENV["DRIVER_PATH"]);
    Server=$(ENV["ACCOUNT"]).snowflakecomputing.com;Warehouse=$(get(ENV, "WAREHOUSE", "USER_WH"));Role=$(get(ENV, "ROLE", "DATA_ANALYST"));Database=$(get(ENV, "DATABASE", "MODEL_DB"));Schema=$(get(ENV, "SCHEMA", "PUBLIC"))", ENV["DBUSER"], ENV["PASSWD"]))
end

function DbConstructor(databaseUrl::String)
    @debug "Opening connection from databaseUrl : $databaseUrl"
    dbClass(ODBC.Connection(databaseUrl, ENV["DBUSER"], ENV["PASSWD"]))
end

function execQuery(db, query::String)
    @debug "Exec Query $query"
    DBInterface.execute(db.conn, query)
end

function execQueryFromFile(db, filePath::String, replaceFromEnv=true)
    query = open(f -> read(f, String), filePath)
    if replaceFromEnv 
        for m in eachmatch(REGEX, query)
            val = get(ENV, chop(m.match, head=2, tail=2), missing)
            query = replace(query, m.match => val)
        end
    end
    @debug "Exec QueryFromFile $query"
    DBInterface.execute(db.conn, query)
end

toChunk(df::DataFrame, n) = [df[i:min(i + n - 1, end),:] for i in 1:n:nrow(df)]

function write(db, df, tableName)
    _prepare_field(x::Any) = x
    _prepare_field(x::Missing) = ""
    _prepare_field(x::Nothing) = ""
    _prepare_field(x::AbstractString) = "'" * replace(x, "'" => '"') * "'"
    _prepare_field(x::AbstractFloat) = isnan(x) ? 0.0 : round(x, digits=2)

    params = join(names(df), ',')
    chunks = toChunk(df, INSERT_CHUNK_LIMIT)
    for chunk in chunks
        rowsStrings = replace(join(map(x -> '(' * join(x, ',') * ')', eachrow(_prepare_field.(chunk))), ','), ",," => ",'',") 
        query = "INSERT INTO $tableName($params) VALUES $rowsStrings"
        if nrow(chunk) > 0
            DBInterface.execute(db.conn, query)
        end
    end
    return nrow(df)
end


function close!(db::dbClass)
    DBInterface.close!(db.conn)
end

end
