#if !USING_BUILTIN_SQLITE
    #if os(OSX)
        import SQLiteMacOSX
    #elseif os(iOS)
        #if (arch(i386) || arch(x86_64))
            import SQLiteiPhoneSimulator
        #else
            import SQLiteiPhoneOS
        #endif
    #endif
#endif


// MARK: - _SQLSelectQuery

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public struct _SQLSelectQuery {
    var selection: [_SQLSelectable]
    var distinct: Bool
    var source: SQLSource?
    var whereExpression: _SQLExpression?
    var groupByExpressions: [_SQLExpression]
    var orderings: [_SQLOrdering]
    var reversed: Bool
    var havingExpression: _SQLExpression?
    var limit: _SQLLimit?
    
    init(
        select selection: [_SQLSelectable],
        distinct: Bool = false,
        from source: SQLSource? = nil,
        filter whereExpression: _SQLExpression? = nil,
        groupBy groupByExpressions: [_SQLExpression] = [],
        orderBy orderings: [_SQLOrdering] = [],
        reversed: Bool = false,
        having havingExpression: _SQLExpression? = nil,
        limit: _SQLLimit? = nil)
    {
        self.selection = selection
        self.distinct = distinct
        self.source = source
        self.whereExpression = whereExpression
        self.groupByExpressions = groupByExpressions
        self.orderings = orderings
        self.reversed = reversed
        self.havingExpression = havingExpression
        self.limit = limit
    }
    
    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String {
        // Prevent source ambiguity
        if let source = source {
            var sourcesByName: [String: [_SQLSource]] = [:]
            for source in source.referencedSources {
                guard let name = source.name else { continue }
                var sources = sourcesByName[name] ?? []
                guard sources.indexOf({ $0 === source }) == nil else {
                    continue
                }
                sources.append(source)
                sourcesByName[name] = sources
            }
            for (name, sources) in sourcesByName where sources.count > 1 {
                for (index, source) in sources.enumerate() {
                    source.name = "\(name)\(index)"
                }
            }
        }
        
        var sql = "SELECT"
        
        if distinct {
            sql += " DISTINCT"
        }
        
        assert(!selection.isEmpty)
        if case .Star(let starSource) = selection[0].sqlSelectableKind where starSource === source {
            sql += " *"
        } else {
            sql += try " " + selection.map { try $0.resultColumnSQL(db, &arguments) }.joinWithSeparator(", ")
        }
        
        if let source = source {
            sql += try " FROM " + source.sql(db, &arguments)
        }
        
        if let whereExpression = whereExpression {
            sql += try " WHERE " + whereExpression.sql(db, &arguments)
        }
        
        if !groupByExpressions.isEmpty {
            sql += try " GROUP BY " + groupByExpressions.map { try $0.sql(db, &arguments) }.joinWithSeparator(", ")
        }
        
        if let havingExpression = havingExpression {
            sql += try " HAVING " + havingExpression.sql(db, &arguments)
        }
        
        var orderings = self.orderings
        if reversed {
            if orderings.isEmpty {
                // https://www.sqlite.org/lang_createtable.html#rowid
                //
                // > The rowid value can be accessed using one of the special
                // > case-independent names "rowid", "oid", or "_rowid_" in
                // > place of a column name. If a table contains a user defined
                // > column named "rowid", "oid" or "_rowid_", then that name
                // > always refers the explicitly declared column and cannot be
                // > used to retrieve the integer rowid value.
                //
                // Here we assume that _rowid_ is not a custom column.
                // TODO: support for user-defined _rowid_ column.
                // TODO: support for WITHOUT ROWID tables.
                orderings = [SQLColumn("_rowid_").desc]
            } else {
                orderings = orderings.map { $0.reversedSortDescriptor }
            }
        }
        if !orderings.isEmpty {
            sql += try " ORDER BY " + orderings.map { try $0.orderingSQL(db, &arguments) }.joinWithSeparator(", ")
        }
        
        if let limit = limit {
            sql += " LIMIT " + limit.sql
        }
        
        return sql
    }
    
    /// Returns a query that counts the number of rows matched by self.
    var countQuery: _SQLSelectQuery {
        guard groupByExpressions.isEmpty && limit == nil else {
            // SELECT ... GROUP BY ...
            // SELECT ... LIMIT ...
            return trivialCountQuery
        }
        
        guard let table = source as? _SQLSourceTable else {
            // SELECT ... FROM (something which is not a table)
            return trivialCountQuery
        }
        
        assert(!selection.isEmpty)
        if selection.count == 1 {
            let selectable = self.selection[0]
            switch selectable.sqlSelectableKind {
            case .Star(source: let source):
                guard !distinct else {
                    return trivialCountQuery
                }
                
                guard source === table else {
                    return trivialCountQuery
                }
                
                // SELECT * FROM tableName ...
                // ->
                // SELECT COUNT(*) FROM tableName ...
                var countQuery = unorderedQuery
                countQuery.selection = [_SQLExpression.Count(selectable)]
                return countQuery
                
            case .Expression(let expression):
                // SELECT [DISTINCT] expr FROM tableName ...
                if distinct {
                    // SELECT DISTINCT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(DISTINCT expr) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.distinct = false
                    countQuery.selection = [_SQLExpression.CountDistinct(expression)]
                    return countQuery
                } else {
                    // SELECT expr FROM tableName ...
                    // ->
                    // SELECT COUNT(*) FROM tableName ...
                    var countQuery = unorderedQuery
                    countQuery.selection = [_SQLExpression.Count(_SQLResultColumn.Star(table))]
                    return countQuery
                }
            }
        } else {
            // SELECT [DISTINCT] expr1, expr2, ... FROM tableName ...
            
            guard !distinct else {
                return trivialCountQuery
            }

            // SELECT expr1, expr2, ... FROM tableName ...
            // ->
            // SELECT COUNT(*) FROM tableName ...
            var countQuery = unorderedQuery
            countQuery.selection = [_SQLExpression.Count(_SQLResultColumn.Star(table))]
            return countQuery
        }
    }
    
    // SELECT COUNT(*) FROM (self)
    private var trivialCountQuery: _SQLSelectQuery {
        let source = _SQLSourceQuery(query: unorderedQuery, name: nil)
        return _SQLSelectQuery(
            select: [_SQLExpression.Count(_SQLResultColumn.Star(source))],
            from: source)
    }
    
    /// Remove ordering
    private var unorderedQuery: _SQLSelectQuery {
        var query = self
        query.reversed = false
        query.orderings = []
        return query
    }
    
    func adapter(statement: SelectStatement) throws -> RowAdapter? {
        guard let source = source else {
            return nil
        }
        // Our sources define variant based on selection index:
        //
        //      SELECT a.*, b.* FROM a JOIN b ...
        //                  ^ variant at selection index 1
        //
        // Now that we have a statement, we can turn those indexes into
        // column indexes:
        //
        //      SELECT a.id, a.name, b.id, b.title FROM a JOIN b ...
        //                           ^ variant at column index 2
        var columnIndex = 0
        var columnIndexForSelectionIndex: [Int: Int] = [:]
        for (selectionIndex, selectable) in selection.enumerate() {
            columnIndexForSelectionIndex[selectionIndex] = columnIndex
            switch selectable.sqlSelectableKind {
            case .Expression:
                columnIndex += 1
            case .Star(let source):
                columnIndex += try source.numberOfColumns(statement.database)
            }
        }
        
        return source.adapter(columnIndexForSelectionIndex)
    }
    
    func numberOfColumns(db: Database) throws -> Int {
        return try selection.reduce(0) { (count, let selectable) in
            switch selectable.sqlSelectableKind {
            case .Expression:
                return count + 1
            case .Star(let source):
                return try count + source.numberOfColumns(db)
            }
        }
    }
}


// MARK: - _SQLSource

/// TODO
public protocol _SQLSource: class {
    var name: String? { get set }
    var referencedSources: [_SQLSource] { get }
    func numberOfColumns(db: Database) throws -> Int
    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String
    func fork() -> Self
    func adapter(columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter?
}

/// TODO
public protocol SQLSource : _SQLSource {
    /// TODO
    func include(required required: Bool, relation: Relation) -> SQLSource
    /// TODO
    func join(required required: Bool, relation: Relation) -> SQLSource
}

extension SQLSource {
    /// TODO
    public subscript(columnName: String) -> SQLColumn {
        return SQLColumn(name: columnName, source: self)
    }
    
    /// TODO
    public subscript(column: SQLColumn) -> SQLColumn {
        return self[column.name]
    }
}

final class _SQLSourceTable {
    private let tableName: String
    var alias: String?
    
    init(tableName: String, alias: String?) {
        self.tableName = tableName
        self.alias = alias
    }
}

extension _SQLSourceTable : _SQLSource {
    
    var name : String? {
        get { return alias ?? tableName }
        set { alias = newValue }
    }
    
    var referencedSources: [_SQLSource] {
        return [self]
    }
    
    func numberOfColumns(db: Database) throws -> Int {
        return try db.numberOfColumns(tableName)
    }
    
    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String {
        if let alias = alias {
            return tableName.quotedDatabaseIdentifier + " " + alias.quotedDatabaseIdentifier
        } else {
            return tableName.quotedDatabaseIdentifier
        }
    }
    
    func fork() -> _SQLSourceTable {
        return _SQLSourceTable(tableName: tableName, alias: alias)
    }
    
    func adapter(columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        return nil
    }
}

extension _SQLSourceTable : SQLSource {
    func include(required required: Bool, relation: Relation) -> SQLSource {
        return _SQLRelationTree(leftSource: self, joins: [Join(included: true, kind: required ? .Inner : .Left, relation: relation)])
    }
    
    func join(required required: Bool, relation: Relation) -> SQLSource {
        return _SQLRelationTree(leftSource: self, joins: [Join(included: false, kind: required ? .Inner : .Left, relation: relation)])
    }
}

final class _SQLSourceQuery {
    private let query: _SQLSelectQuery
    var name: String?
    
    init(query: _SQLSelectQuery, name: String?) {
        self.query = query
        self.name = name
    }
}

extension _SQLSourceQuery: _SQLSource {
    
    var referencedSources: [_SQLSource] {
        if let source = query.source {
            return [self] + source.referencedSources
        }
        return [self]
    }
    
    func numberOfColumns(db: Database) throws -> Int {
        return try query.numberOfColumns(db)
    }

    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String {
        if let name = name {
            return try "(" + query.sql(db, &arguments) + ") AS " + name.quotedDatabaseIdentifier
        } else {
            return try "(" + query.sql(db, &arguments) + ")"
        }
    }
    
    func fork() -> _SQLSourceQuery {
        return _SQLSourceQuery(query: query, name: name)
    }
    
    func adapter(columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        return nil
    }
}

extension _SQLSourceQuery : SQLSource {
    // TODO
    func include(required required: Bool, relation: Relation) -> SQLSource {
        return _SQLRelationTree(leftSource: self, joins: [Join(included: true, kind: required ? .Inner : .Left, relation: relation)])
    }
    
    // TODO
    func join(required required: Bool, relation: Relation) -> SQLSource {
        return _SQLRelationTree(leftSource: self, joins: [Join(included: false, kind: required ? .Inner : .Left, relation: relation)])
    }
}

final class _SQLRelationTree {
    private let leftSource: SQLSource
    private let joins: [Join]
    
    init(leftSource: SQLSource, joins: [Join]) {
        self.leftSource = leftSource
        self.joins = joins
    }
}

extension _SQLRelationTree : _SQLSource {
    var name : String? {
        get { return leftSource.name }
        set { leftSource.name = newValue }
    }
    
    var referencedSources: [_SQLSource] {
        var result = leftSource.referencedSources
        for join in joins {
            result += join.relation.referencedSources
        }
        return result
    }
    
    func numberOfColumns(db: Database) throws -> Int {
        var result = try leftSource.numberOfColumns(db)
        for join in joins {
            result += try join.numberOfColumns(db)
        }
        return result
    }
    
    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String {
        var sql = try leftSource.sql(db, &arguments)
        for join in joins {
            sql += try " " + join.relation.sql(db, &arguments, leftSource: leftSource, joinKind: join.kind, innerJoinForbidden: false)
        }
        return sql
    }
    
    func fork() -> _SQLRelationTree {
        return _SQLRelationTree(leftSource: leftSource.fork(), joins: joins.map { $0.fork() })
    }
    
    func adapter(columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        var selectionIndex = 1
        var variants: [String: RowAdapter] = [:]
        for join in joins {
            if let adapter = join.relation.adapter(included: join.included, selectionIndex: &selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex) {
                variants[join.relation.variantName] = adapter
            }
        }
        if variants.isEmpty { return nil }
        return SuffixRowAdapter(fromIndex: 0).adapterWithVariants(variants)
    }
}

extension _SQLRelationTree : SQLSource {
    
    func include(required required: Bool, relation: Relation) -> SQLSource {
        var joins = self.joins
        joins.append(Join(included: true, kind: required ? .Inner : .Left, relation: relation))
        return _SQLRelationTree(leftSource: leftSource, joins: joins)
    }
    
    func join(required required: Bool, relation: Relation) -> SQLSource {
        var joins = self.joins
        joins.append(Join(included: false, kind: required ? .Inner : .Left, relation: relation))
        return _SQLRelationTree(leftSource: leftSource, joins: joins)
    }
}


// MARK: - _SQLOrdering

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLOrdering {
    var reversedSortDescriptor: _SQLSortDescriptor { get }
    func orderingSQL(db: Database, inout _ arguments: StatementArguments) throws -> String
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLSortDescriptor {
    case Asc(_SQLExpression)
    case Desc(_SQLExpression)
}

extension _SQLSortDescriptor : _SQLOrdering {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedSortDescriptor: _SQLSortDescriptor {
        switch self {
        case .Asc(let expression):
            return .Desc(expression)
        case .Desc(let expression):
            return .Asc(expression)
        }
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        switch self {
        case .Asc(let expression):
            return try expression.sql(db, &arguments) + " ASC"
        case .Desc(let expression):
            return try expression.sql(db, &arguments) + " DESC"
        }
    }
}


// MARK: - _SQLLimit

struct _SQLLimit {
    let limit: Int
    let offset: Int?
    
    var sql: String {
        if let offset = offset {
            return "\(limit) OFFSET \(offset)"
        } else {
            return "\(limit)"
        }
    }
}


// MARK: - _SQLExpressible

public protocol _SQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    var sqlExpression: _SQLExpression { get }
}

// Conformance to _SQLExpressible
extension DatabaseValueConvertible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .Value(self)
    }
}

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SpecificSQLExpressible : _SQLExpressible {
    // _SQLExpressible can be adopted by Swift standard types, and user
    // types, through the DatabaseValueConvertible protocol, which inherits
    // from _SQLExpressible.
    //
    // For example, Int adopts _SQLExpressible through
    // DatabaseValueConvertible.
    //
    // _SpecificSQLExpressible, on the other side, is not adopted by any
    // Swift standard type or any user type. It is only adopted by GRDB types,
    // such as SQLColumn and _SQLExpression.
    //
    // This separation lets us define functions and operators that do not
    // spill out. The three declarations below have no chance overloading a
    // Swift-defined operator, or a user-defined operator:
    //
    // - ==(_SQLExpressible, _SpecificSQLExpressible)
    // - ==(_SpecificSQLExpressible, _SQLExpressible)
    // - ==(_SpecificSQLExpressible, _SpecificSQLExpressible)
}

extension _SpecificSQLExpressible where Self: _SQLOrdering {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var reversedSortDescriptor: _SQLSortDescriptor {
        return .Desc(sqlExpression)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func orderingSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        return try sqlExpression.sql(db, &arguments)
    }
}

extension _SpecificSQLExpressible where Self: _SQLSelectable {
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func resultColumnSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        return try sqlExpression.sql(db, &arguments)
    }
    
    /// This method is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func countedSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        return try sqlExpression.sql(db, &arguments)
    }
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlSelectableKind: _SQLSelectableKind {
        return .Expression(sqlExpression)
    }
}

extension _SpecificSQLExpressible {
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var asc: _SQLSortDescriptor {
        return .Asc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.order()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var desc: _SQLSortDescriptor {
        return .Desc(sqlExpression)
    }
    
    /// Returns a value that can be used as an argument to FetchRequest.select()
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public func aliased(alias: String) -> _SQLSelectable {
        return _SQLResultColumn.Expression(expression: sqlExpression, alias: alias)
    }
}


/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public indirect enum _SQLExpression {
    /// For example: `name || 'rrr' AS pirateName`
    case Literal(String, StatementArguments?)
    
    /// For example: `1` or `'foo'`
    case Value(DatabaseValueConvertible?)   // TODO: switch to DatabaseValue?
    
    /// For example: `name`, `table.name`
    case Identifier(identifier: String, sourceName: String?)
    
    /// For example: `name = 'foo' COLLATE NOCASE`
    case Collate(_SQLExpression, String)
    
    /// For example: `NOT condition`
    case Not(_SQLExpression)
    
    /// For example: `name = 'foo'`
    case Equal(_SQLExpression, _SQLExpression)
    
    /// For example: `name <> 'foo'`
    case NotEqual(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NULL`
    case Is(_SQLExpression, _SQLExpression)
    
    /// For example: `name IS NOT NULL`
    case IsNot(_SQLExpression, _SQLExpression)
    
    /// For example: `-value`
    case PrefixOperator(String, _SQLExpression)
    
    /// For example: `age + 1`
    case InfixOperator(String, _SQLExpression, _SQLExpression)
    
    /// For example: `id IN (1,2,3)`
    case In([_SQLExpression], _SQLExpression)
    
    /// For example `id IN (SELECT ...)`
    case InSubQuery(_SQLSelectQuery, _SQLExpression)
    
    /// For example `EXISTS (SELECT ...)`
    case Exists(_SQLSelectQuery)
    
    /// For example: `age BETWEEN 1 AND 2`
    case Between(value: _SQLExpression, min: _SQLExpression, max: _SQLExpression)
    
    /// For example: `LOWER(name)`
    case Function(String, [_SQLExpression])
    
    /// For example: `COUNT(*)`
    case Count(_SQLSelectable)
    
    /// For example: `COUNT(DISTINCT name)`
    case CountDistinct(_SQLExpression)
    
    ///
    func sql(db: Database, inout _ arguments: StatementArguments) throws -> String {
        // NOTE: this method *was* slow to compile
        // https://medium.com/swift-programming/speeding-up-slow-swift-build-times-922feeba5780#.s77wmh4h0
        // 10746.4ms	/Users/groue/Documents/git/groue/GRDB.swift/GRDB/FetchRequest/SQLSelectQuery.swift:439:10	func sql(db: Database, inout _ arguments: StatementArguments) throws -> String
        // Fixes are marked with "## Slow Compile Fix (Swift 2.2.x):"
        //
        switch self {
        case .Literal(let sql, let literalArguments):
            if let literalArguments = literalArguments {
                arguments.values.appendContentsOf(literalArguments.values)
                for (name, value) in literalArguments.namedValues {
                    if arguments.namedValues[name] != nil {
                        throw DatabaseError(code: SQLITE_MISUSE, message: "argument \(String(reflecting: name)) can't be reused")
                    }
                    arguments.namedValues[name] = value
                }
            }
            return sql
            
        case .Value(let value):
            guard let value = value else {
                return "NULL"
            }
            arguments.values.append(value)
            return "?"
            
        case .Identifier(let identifier, let sourceName):
            if let sourceName = sourceName {
                return sourceName.quotedDatabaseIdentifier + "." + identifier.quotedDatabaseIdentifier
            } else {
                return identifier.quotedDatabaseIdentifier
            }
            
        case .Collate(let expression, let collation):
            let sql = try expression.sql(db, &arguments)
            let chars = sql.characters
            if chars.last! == ")" {
                return String(chars.prefixUpTo(chars.endIndex.predecessor())) + " COLLATE " + collation + ")"
            } else {
                return sql + " COLLATE " + collation
            }
            
        case .Not(let condition):
            switch condition {
            case .Not(let expression):
                return try expression.sql(db, &arguments)
                
            case .In(let expressions, let expression):
                if expressions.isEmpty {
                    return "1"
                } else {
                    // ## Slow Compile Fix (Swift 2.2.x):
                    // TODO: Check if Swift 3 compiler fixes this line's slow compilation time:
                    //return try "(" + expression.sql(db, &arguments) + " NOT IN (" + expressions.map { try $0.sql(db, &arguments) }.joinWithSeparator(", ") + "))"   // Original, Slow To Compile
                    return try "(" + expression.sql(db, &arguments) + " NOT IN (" + (expressions.map { try $0.sql(db, &arguments) } as [String]).joinWithSeparator(", ") + "))"
                }
                
            case .InSubQuery(let subQuery, let expression):
                return try "(" + expression.sql(db, &arguments) + " NOT IN (" + subQuery.sql(db, &arguments)  + "))"
                
            case .Exists(let subQuery):
                return try "(NOT EXISTS (" + subQuery.sql(db, &arguments)  + "))"
                
            case .Equal(let lhs, let rhs):
                return try _SQLExpression.NotEqual(lhs, rhs).sql(db, &arguments)
                
            case .NotEqual(let lhs, let rhs):
                return try _SQLExpression.Equal(lhs, rhs).sql(db, &arguments)
                
            case .Is(let lhs, let rhs):
                return try _SQLExpression.IsNot(lhs, rhs).sql(db, &arguments)
                
            case .IsNot(let lhs, let rhs):
                return try _SQLExpression.Is(lhs, rhs).sql(db, &arguments)
                
            default:
                return try "(NOT " + condition.sql(db, &arguments) + ")"
            }
            
        case .Equal(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs == nil:
                // Swiftism!
                // Turn `filter(a == nil)` into `a IS NULL` since the intention is obviously to check for NULL. `a = NULL` would evaluate to NULL.
                return try "(" + lhs.sql(db, &arguments) + " IS NULL)"
            case (.Value(let lhs), let rhs) where lhs == nil:
                // Swiftism!
                return try "(" + rhs.sql(db, &arguments) + " IS NULL)"
            default:
                return try "(" + lhs.sql(db, &arguments) + " = " + rhs.sql(db, &arguments) + ")"
            }
            
        case .NotEqual(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs == nil:
                // Swiftism!
                // Turn `filter(a != nil)` into `a IS NOT NULL` since the intention is obviously to check for NULL. `a <> NULL` would evaluate to NULL.
                return try "(" + lhs.sql(db, &arguments) + " IS NOT NULL)"
            case (.Value(let lhs), let rhs) where lhs == nil:
                // Swiftism!
                return try "(" + rhs.sql(db, &arguments) + " IS NOT NULL)"
            default:
                return try "(" + lhs.sql(db, &arguments) + " <> " + rhs.sql(db, &arguments) + ")"
            }
            
        case .Is(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs == nil:
                return try "(" + lhs.sql(db, &arguments) + " IS NULL)"
            case (.Value(let lhs), let rhs) where lhs == nil:
                return try "(" + rhs.sql(db, &arguments) + " IS NULL)"
            default:
                return try "(" + lhs.sql(db, &arguments) + " IS " + rhs.sql(db, &arguments) + ")"
            }
            
        case .IsNot(let lhs, let rhs):
            switch (lhs, rhs) {
            case (let lhs, .Value(let rhs)) where rhs == nil:
                return try "(" + lhs.sql(db, &arguments) + " IS NOT NULL)"
            case (.Value(let lhs), let rhs) where lhs == nil:
                return try "(" + rhs.sql(db, &arguments) + " IS NOT NULL)"
            default:
                return try "(" + lhs.sql(db, &arguments) + " IS NOT " + rhs.sql(db, &arguments) + ")"
            }
            
        case .PrefixOperator(let SQLOperator, let value):
            return try SQLOperator + value.sql(db, &arguments)
            
        case .InfixOperator(let SQLOperator, let lhs, let rhs):
            return try "(" + lhs.sql(db, &arguments) + " \(SQLOperator) " + rhs.sql(db, &arguments) + ")"
            
        case .In(let expressions, let expression):
            guard !expressions.isEmpty else {
                return "0"
            }
            // ## Slow Compile Fix (Swift 2.2.x):
            // TODO: Check if Swift 3 compiler fixes this line's slow compilation time:
            //return try "(" + expression.sql(db, &arguments) + " IN (" + expressions.map { try $0.sql(db, &arguments) }.joinWithSeparator(", ")  + "))"  // Original, Slow To Compile
            return try "(" + expression.sql(db, &arguments) + " IN (" + (expressions.map { try $0.sql(db, &arguments) } as [String]).joinWithSeparator(", ")  + "))"
        
        case .InSubQuery(let subQuery, let expression):
            return try "(" + expression.sql(db, &arguments) + " IN (" + subQuery.sql(db, &arguments)  + "))"
            
        case .Exists(let subQuery):
            return try "(EXISTS (" + subQuery.sql(db, &arguments)  + "))"
            
        case .Between(value: let value, min: let min, max: let max):
            return try "(" + value.sql(db, &arguments) + " BETWEEN " + min.sql(db, &arguments) + " AND " + max.sql(db, &arguments) + ")"
            
        case .Function(let functionName, let functionArguments):
            // ## Slow Compile Fix (Swift 2.2.x):
            // TODO: Check if Swift 3 compiler fixes this line's slow compilation time:
            //return try functionName + "(" + functionArguments.map { try $0.sql(db, &arguments) }.joinWithSeparator(", ")  + ")"    // Original, Slow To Compile
            return try functionName + "(" + (functionArguments.map { try $0.sql(db, &arguments) } as [String]).joinWithSeparator(", ")  + ")"
            
        case .Count(let counted):
            return try "COUNT(" + counted.countedSQL(db, &arguments) + ")"
            
        case .CountDistinct(let expression):
            return try "COUNT(DISTINCT " + expression.sql(db, &arguments) + ")"
        }
    }
}

extension _SQLExpression : _SpecificSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return self
    }
}

extension _SQLExpression : _SQLSelectable {}
extension _SQLExpression : _SQLOrdering {}


// MARK: - _SQLSelectable

/// This protocol is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public protocol _SQLSelectable {
    func resultColumnSQL(db: Database, inout _ arguments: StatementArguments) throws -> String
    func countedSQL(db: Database, inout _ arguments: StatementArguments) throws -> String
    var sqlSelectableKind: _SQLSelectableKind { get }
}

/// This type is an implementation detail of the query interface.
/// Do not use it directly.
///
/// See https://github.com/groue/GRDB.swift/#the-query-interface
public enum _SQLSelectableKind {
    case Star(_SQLSource)
    case Expression(_SQLExpression)
}

enum _SQLResultColumn {
    case Star(_SQLSource)
    case Expression(expression: _SQLExpression, alias: String)
}

extension _SQLResultColumn : _SQLSelectable {
    
    func resultColumnSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        switch self {
        case .Star(let source):
            if let sourceName = source.name {
                return sourceName.quotedDatabaseIdentifier + ".*"
            } else {
                return "*"
            }
        case .Expression(expression: let expression, alias: let alias):
            return try expression.sql(db, &arguments) + " AS " + alias.quotedDatabaseIdentifier
        }
    }
    
    func countedSQL(db: Database, inout _ arguments: StatementArguments) throws -> String {
        switch self {
        case .Star:
            return "*"
        case .Expression(expression: let expression, alias: _):
            return try expression.sql(db, &arguments)
        }
    }
    
    var sqlSelectableKind: _SQLSelectableKind {
        switch self {
        case .Star(let source):
            return .Star(source)
        case .Expression(expression: let expression, alias: _):
            return .Expression(expression)
        }
    }
}


// MARK: - SQLColumn

/// A column in the database
///
/// See https://github.com/groue/GRDB.swift#the-query-interface
public struct SQLColumn {
    /// The name of the column
    public let name: String
    let source: _SQLSource?
    
    /// Initializes a column given its name.
    public init(_ name: String) {
        self.name = name
        self.source = nil
    }
    
    init(name: String, source: _SQLSource) {
        self.name = name
        self.source = source
    }
}

extension SQLColumn : _SpecificSQLExpressible {
    
    /// This property is an implementation detail of the query interface.
    /// Do not use it directly.
    ///
    /// See https://github.com/groue/GRDB.swift/#the-query-interface
    public var sqlExpression: _SQLExpression {
        return .Identifier(identifier: name, sourceName: source?.name)
    }
}

extension SQLColumn : _SQLSelectable {}
extension SQLColumn : _SQLOrdering {}
