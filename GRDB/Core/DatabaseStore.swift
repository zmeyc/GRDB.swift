import Foundation
#if os(Linux)
    import Dispatch
#endif

#if os(Linux)
    public typealias FileAttributeKey = String
#endif

/// DatabaseStore is responsible for applying Configuration.fileAttributes to
/// all database files (db.sqlite, db.sqlite-wal, db.sqlite-shm,
/// db.sqlite-journal, etc.)
class DatabaseStore {
    let path: String
    #if !os(Linux)
    private let source: DispatchSourceFileSystemObject?
    private let queue: DispatchQueue?
    #endif
    
    init(path: String, attributes: [FileAttributeKey: Any]?) throws {
        self.path = path
        
        guard let attributes = attributes else {
            #if !os(Linux)
            self.queue = nil
            self.source = nil
            #endif
            return
        }
        
        let pathUrl = NSURL(fileURLWithPath: path)
        let databaseFileName = pathUrl.lastPathComponent ?? ""
        let directoryPath = pathUrl.deletingLastPathComponent?.path ?? ""
        
        // Apply file attributes on existing files
        DatabaseStore.setFileAttributes(
            directoryPath: directoryPath,
            databaseFileName: databaseFileName,
            attributes: attributes)
        
        #if os(Linux)
        print("WARNING: file monitoring is not implemented on Linux, attributes won't be applied to newly created files")
        #else
        // We use a dispatch_source to monitor the contents of the database file
        // parent directory, and apply file attributes.
        //
        // This require a file descriptor on the directory.
        let directoryDescriptor = open(directoryPath, O_EVTONLY)
        guard directoryDescriptor != -1 else {
            // Let FileManager throw a nice NSError
            try FileManager.default.contentsOfDirectory(atPath: directoryPath)
            // Come on, FileManager... OK just throw something that is somewhat relevant
            throw NSError(domain: NSCocoaErrorDomain, code: NSFileReadUnknownError, userInfo: nil)
        }
        
        let queue = DispatchQueue(label: "GRDB.DatabaseStore")
        let source = DispatchSource.makeFileSystemObjectSource(fileDescriptor: Int32(directoryDescriptor), eventMask: [.write], queue: queue)
        self.queue = queue
        self.source = source
        
        // Configure dispatch source
        source.setEventHandler {
            // Directory has been modified: apply file attributes on unprocessed files
            DatabaseStore.setFileAttributes(
                directoryPath: directoryPath,
                databaseFileName: databaseFileName,
                attributes: attributes)
        }
        source.setCancelHandler {
            close(directoryDescriptor)
        }
        source.resume()
        #endif
    }
    
    deinit {
        #if !os(Linux)
        if let source = source {
            source.cancel()
        }
        #endif
    }
    
    private static func setFileAttributes(directoryPath: String, databaseFileName: String, attributes: [FileAttributeKey: Any]) {
        let fm = FileManager.default
        // TODO: handle symbolic links:
        //
        // According to https://www.sqlite.org/changes.html
        // > 2016-01-06 (3.10.0)
        // > On unix, if a symlink to a database file is opened, then the
        // > corresponding journal files are based on the actual filename,
        // > not the symlink name.
        let fileNames = try! fm.contentsOfDirectory(atPath: directoryPath).filter({ $0.hasPrefix(databaseFileName) })
        for fileName in fileNames {
            do {
                let directoryPathUrl = NSURL(fileURLWithPath: directoryPath)
                let path = directoryPathUrl.appendingPathComponent(fileName)?.path ?? ""
                try fm.setAttributes(attributes, ofItemAtPath: path)
            } catch let error as NSError {
                #if os(Linux)
                let noSuchFileError = NSCocoaError.FileNoSuchFileError.rawValue
                #else
                let noSuchFileError = NSFileNoSuchFileError
                #endif
                guard error.domain == NSCocoaErrorDomain && error.code == noSuchFileError else {
                    try! { throw error }()
                    preconditionFailure()
                }
            }
        }
    }
}
