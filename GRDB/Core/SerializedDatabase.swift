import Foundation
#if os(Linux)
    import Dispatch
#endif

/// A class that serializes accesses to a database.
final class SerializedDatabase {
    /// The database connection
    private let db: Database
    
    /// The database configuration
    var configuration: Configuration {
        return db.configuration
    }
    
    /// The dispatch queue
    private let queue: DispatchQueue
    
    init(path: String, configuration: Configuration = Configuration(), schemaCache: DatabaseSchemaCache) throws {
        // According to https://www.sqlite.org/threadsafe.html
        //
        // > SQLite support three different threading modes:
        // >
        // > 1. Multi-thread. In this mode, SQLite can be safely used by
        // >    multiple threads provided that no single database connection is
        // >    used simultaneously in two or more threads.
        // >
        // > 2. Serialized. In serialized mode, SQLite can be safely used by
        // >    multiple threads with no restriction.
        // >
        // > [...]
        // >
        // > The default mode is serialized.
        //
        // Since our database connection is only used via our serial dispatch
        // queue, there is no purpose using the default serialized mode.
        var config = configuration
        config.threadingMode = .multiThread
        
        db = try Database(path: path, configuration: config, schemaCache: schemaCache)
        queue = SchedulingWatchdog.makeSerializedQueueAllowing(database: db)
        try queue.sync {
            try db.setup()
        }
    }
    
    deinit {
        sync { db in
            db.close()
        }
    }
    
    /// Synchronously executes a block the serialized dispatch queue, and returns
    /// its result.
    ///
    /// This method is *not* reentrant.
    func sync<T>(_ block: @noescape (_ db: Database) throws -> T) rethrows -> T {
        // Three different cases:
        //
        // 1. A database is invoked from some queue like the main queue:
        //
        //      dbQueue.inDatabase { db in
        //      }
        //
        // 2. A database is invoked in a reentrant way:
        //
        //      dbQueue.inDatabase { db in
        //          dbQueue.inDatabase { db in
        //          }
        //      }
        //
        // 3. A database in invoked from another database:
        //
        //      dbQueue1.inDatabase { db1 in
        //          dbQueue2.inDatabase { db2 in
        //          }
        //      }
        
        if let watchdog = SchedulingWatchdog.current {
            // Case 2 or 3:
            //
            // 2. A database is invoked in a reentrant way:
            //
            //      dbQueue.inDatabase { db in
            //          dbQueue.inDatabase { db in
            //          }
            //      }
            //
            // 3. A database in invoked from another database:
            //
            //      dbQueue1.inDatabase { db1 in
            //          dbQueue2.inDatabase { db2 in
            //          }
            //      }
            //
            // 2 is forbidden.
            GRDBPrecondition(!watchdog.allows(db), "Database methods are not reentrant.")
            
            // Case 3:
            //
            // 3. A database in invoked from another database:
            //
            //      dbQueue1.inDatabase { db1 in
            //          dbQueue2.inDatabase { db2 in
            //          }
            //      }
            //
            // Let's enter the new queue, and temporarily allow the
            // currently allowed databases inside.
            return try queue.sync {
                try SchedulingWatchdog.current!.allowing(databases: watchdog.allowedDatabases) {
                    try block(db)
                }
            }
        } else {
            // Case 1:
            //
            // 1. A database is invoked from some queue like the main queue:
            //
            //      dbQueue.inDatabase { db in
            //      }
            //
            // Just dispatch block to queue:
            return try queue.sync {
                try block(db)
            }
        }
    }
    
    /// Asynchronously executes a block in the serialized dispatch queue.
    func async(_ block: @escaping (_ db: Database) -> Void) {
        queue.async {
            block(self.db)
        }
    }
    
    /// Executes the block in the current queue.
    ///
    /// - precondition: the current dispatch queue is valid.
    func execute<T>(_ block: @noescape (_ db: Database) throws -> T) rethrows -> T {
        preconditionValidQueue()
        return try block(db)
    }
    
    /// Fatal error if current dispatch queue is not valid.
    func preconditionValidQueue(_ message: @autoclosure() -> String = "Database was not used on the correct thread.", file: StaticString = #file, line: UInt = #line) {
        SchedulingWatchdog.preconditionValidQueue(db, message, file: file, line: line)
    }
}
