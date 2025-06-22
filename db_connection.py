import mysql.connector
from mysql.connector import pooling, Error
import os
from contextlib import contextmanager
import logging
from typing import Optional, Dict, Any
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration class with environment variable support"""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', '2005'),
            'database': os.getenv('DB_NAME', 'library'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'autocommit': False,
            'raise_on_warnings': True,
            'sql_mode': 'TRADITIONAL',
            'time_zone': '+00:00'
        }
        
        # Connection pool configuration
        self.pool_config = {
            'pool_name': 'library_pool',
            'pool_size': 10,
            'pool_reset_session': True,
            'autocommit': False
        }
        
        # Merge configs
        self.pool_config.update(self.config)

class DatabaseManager:
    """Enhanced database manager with connection pooling and error handling"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(**self.config.pool_config)
            logger.info("✅ Database connection pool initialized successfully")
        except Error as e:
            logger.error(f"❌ Failed to initialize connection pool: {e}")
            raise
    
    def get_connection(self) -> mysql.connector.MySQLConnection:
        """Get connection from pool with retry logic"""
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                if self.connection_pool:
                    connection = self.connection_pool.get_connection()
                    if connection.is_connected():
                        logger.debug(f"✅ Connection obtained from pool (attempt {attempt + 1})")
                        return connection
                else:
                    # Fallback to direct connection
                    connection = mysql.connector.connect(**self.config.config)
                    if connection.is_connected():
                        logger.debug(f"✅ Direct connection established (attempt {attempt + 1})")
                        return connection
                        
            except Error as e:
                logger.warning(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"❌ All connection attempts failed")
                    raise
        
        raise Error("Unable to establish database connection after multiple attempts")
    
    @contextmanager
    def get_connection_context(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection = self.get_connection()
            yield connection
        except Error as e:
            if connection:
                connection.rollback()
            logger.error(f"❌ Database operation failed: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                logger.debug("🔒 Connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     fetch: bool = False, fetch_all: bool = True) -> Any:
        """Execute query with proper error handling and connection management"""
        try:
            with self.get_connection_context() as connection:
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query, params)
                
                if fetch:
                    result = cursor.fetchall() if fetch_all else cursor.fetchone()
                    logger.debug(f"📊 Query executed successfully, {cursor.rowcount} rows affected")
                    return result
                else:
                    connection.commit()
                    logger.debug(f"✅ Query executed successfully, {cursor.rowcount} rows affected")
                    return cursor.rowcount
                    
        except Error as e:
            logger.error(f"❌ Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def execute_many(self, query: str, params_list: list) -> int:
        """Execute multiple queries in a single transaction"""
        try:
            with self.get_connection_context() as connection:
                cursor = connection.cursor()
                cursor.executemany(query, params_list)
                connection.commit()
                logger.info(f"✅ Batch query executed successfully, {cursor.rowcount} rows affected")
                return cursor.rowcount
                
        except Error as e:
            logger.error(f"❌ Batch query execution failed: {e}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return connection info"""
        try:
            with self.get_connection_context() as connection:
                cursor = connection.cursor()
                
                # Get server info
                cursor.execute("SELECT VERSION() as version")
                version_info = cursor.fetchone()
                
                # Get database info
                cursor.execute("SELECT DATABASE() as current_db")
                db_info = cursor.fetchone()
                
                # Get connection info
                connection_info = {
                    'status': 'Connected',
                    'host': connection.server_host,
                    'port': connection.server_port,
                    'user': connection.user,
                    'database': db_info[0] if db_info[0] else 'No database selected',
                    'server_version': version_info[0] if version_info else 'Unknown',
                    'charset': connection.charset,
                    'autocommit': connection.autocommit,
                    'in_transaction': connection.in_transaction
                }
                
                logger.info("✅ Database connection test successful")
                return connection_info
                
        except Error as e:
            logger.error(f"❌ Database connection test failed: {e}")
            return {
                'status': 'Failed',
                'error': str(e)
            }
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            query = """
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """
            result = self.execute_query(
                query, 
                (self.config.config['database'], table_name), 
                fetch=True, 
                fetch_all=False
            )
            return result['count'] > 0 if result else False
            
        except Error as e:
            logger.error(f"❌ Error checking table existence: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[list]:
        """Get table structure information"""
        try:
            query = f"DESCRIBE {table_name}"
            return self.execute_query(query, fetch=True)
        except Error as e:
            logger.error(f"❌ Error getting table info: {e}")
            return None
    
    def close_pool(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            try:
                # Close all connections in pool
                while True:
                    try:
                        conn = self.connection_pool.get_connection()
                        conn.close()
                    except:
                        break
                logger.info("🔒 Connection pool closed")
            except Exception as e:
                logger.error(f"❌ Error closing connection pool: {e}")

# Global database manager instance
db_manager = DatabaseManager()

def create_connection() -> mysql.connector.MySQLConnection:
    """
    Legacy function for backward compatibility
    Returns a database connection
    """
    return db_manager.get_connection()

def execute_query(query: str, params: Optional[tuple] = None, 
                 fetch: bool = False, fetch_all: bool = True) -> Any:
    """
    Execute a database query with proper error handling
    
    Args:
        query: SQL query string
        params: Query parameters tuple
        fetch: Whether to fetch results
        fetch_all: Whether to fetch all results or just one
    
    Returns:
        Query results or row count
    """
    return db_manager.execute_query(query, params, fetch, fetch_all)

def test_connection() -> Dict[str, Any]:
    """Test database connection and return connection info"""
    return db_manager.test_connection()

def initialize_database():
    """Initialize database with required tables"""
    tables = {
        'Books': """
            CREATE TABLE IF NOT EXISTS Books (
                BookID INT AUTO_INCREMENT PRIMARY KEY,
                Title VARCHAR(255) NOT NULL,
                Author VARCHAR(255) NOT NULL,
                PublishedYear INT,
                AvailableCopies INT DEFAULT 1,
                Genre VARCHAR(100),
                ISBN VARCHAR(20) UNIQUE,
                Description TEXT,
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_title (Title),
                INDEX idx_author (Author),
                INDEX idx_genre (Genre)
            )
        """,
        'Members': """
            CREATE TABLE IF NOT EXISTS Members (
                MemberID INT AUTO_INCREMENT PRIMARY KEY,
                Name VARCHAR(255) NOT NULL,
                Email VARCHAR(255) UNIQUE NOT NULL,
                PhoneNumber VARCHAR(20),
                Address TEXT,
                JoinDate DATE DEFAULT (CURRENT_DATE),
                Status ENUM('Active', 'Inactive', 'Suspended') DEFAULT 'Active',
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_email (Email),
                INDEX idx_name (Name)
            )
        """,
        'Borrowing': """
            CREATE TABLE IF NOT EXISTS Borrowing (
                BorrowID INT AUTO_INCREMENT PRIMARY KEY,
                MemberID INT NOT NULL,
                BookID INT NOT NULL,
                BorrowDate DATE DEFAULT (CURRENT_DATE),
                DueDate DATE,
                ReturnDate DATE,
                Status ENUM('Borrowed', 'Returned', 'Overdue') GENERATED ALWAYS AS (
                    CASE 
                        WHEN ReturnDate IS NOT NULL THEN 'Returned'
                        WHEN DueDate < CURRENT_DATE THEN 'Overdue'
                        ELSE 'Borrowed'
                    END
                ) STORED,
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (MemberID) REFERENCES Members(MemberID) ON DELETE CASCADE,
                FOREIGN KEY (BookID) REFERENCES Books(BookID) ON DELETE CASCADE,
                INDEX idx_member (MemberID),
                INDEX idx_book (BookID),
                INDEX idx_status (Status),
                INDEX idx_dates (BorrowDate, DueDate, ReturnDate)
            )
        """
    }
    
    try:
        for table_name, create_query in tables.items():
            if not db_manager.check_table_exists(table_name):
                db_manager.execute_query(create_query)
                logger.info(f"✅ Table '{table_name}' created successfully")
            else:
                logger.info(f"ℹ️ Table '{table_name}' already exists")
        
        logger.info("🎉 Database initialization completed successfully")
        return True
        
    except Error as e:
        logger.error(f"❌ Database initialization failed: {e}")
        return False

def create_sample_data():
    """Create sample data for testing"""
    try:
        # Sample books
        books_data = [
            ("The Great Gatsby", "F. Scott Fitzgerald", 1925, 3, "Fiction", "978-0-7432-7356-5", "A classic American novel"),
            ("To Kill a Mockingbird", "Harper Lee", 1960, 2, "Fiction", "978-0-06-112008-4", "A gripping tale of racial injustice"),
            ("1984", "George Orwell", 1949, 4, "Dystopian", "978-0-452-28423-4", "A dystopian social science fiction novel"),
            ("Pride and Prejudice", "Jane Austen", 1813, 2, "Romance", "978-0-14-143951-8", "A romantic novel of manners"),
            ("The Catcher in the Rye", "J.D. Salinger", 1951, 1, "Fiction", "978-0-316-76948-0", "A controversial coming-of-age story")
        ]
        
        book_query = """
            INSERT IGNORE INTO Books (Title, Author, PublishedYear, AvailableCopies, Genre, ISBN, Description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        rows_affected = db_manager.execute_many(book_query, books_data)
        logger.info(f"✅ {rows_affected} sample books inserted")
        
        # Sample members
        members_data = [
            ("John Doe", "john.doe@email.com", "+1-555-0101", "123 Main St, Anytown, USA"),
            ("Jane Smith", "jane.smith@email.com", "+1-555-0102", "456 Oak Ave, Somewhere, USA"),
            ("Bob Johnson", "bob.johnson@email.com", "+1-555-0103", "789 Pine Rd, Elsewhere, USA"),
            ("Alice Brown", "alice.brown@email.com", "+1-555-0104", "321 Elm St, Nowhere, USA"),
            ("Charlie Wilson", "charlie.wilson@email.com", "+1-555-0105", "654 Maple Dr, Anywhere, USA")
        ]
        
        member_query = """
            INSERT IGNORE INTO Members (Name, Email, PhoneNumber, Address)
            VALUES (%s, %s, %s, %s)
        """
        
        rows_affected = db_manager.execute_many(member_query, members_data)
        logger.info(f"✅ {rows_affected} sample members inserted")
        
        return True
        
    except Error as e:
        logger.error(f"❌ Failed to create sample data: {e}")
        return False

# Test and initialization functions
if __name__ == "__main__":
    print("🔧 Testing Enhanced Database Connection Module")
    print("=" * 50)
    
    # Test connection
    print("\n1. Testing Database Connection...")
    connection_info = test_connection()
    
    if connection_info['status'] == 'Connected':
        print("✅ Database connection successful!")
        print(f"   Host: {connection_info['host']}:{connection_info['port']}")
        print(f"   User: {connection_info['user']}")
        print(f"   Database: {connection_info['database']}")
        print(f"   Server Version: {connection_info['server_version']}")
        print(f"   Charset: {connection_info['charset']}")
    else:
        print("❌ Database connection failed!")
        print(f"   Error: {connection_info.get('error', 'Unknown error')}")
        exit(1)
    
    # Initialize database
    print("\n2. Initializing Database Tables...")
    if initialize_database():
        print("✅ Database tables initialized successfully!")
    else:
        print("❌ Database initialization failed!")
        exit(1)
    
    # Create sample data
    print("\n3. Creating Sample Data...")
    if create_sample_data():
        print("✅ Sample data created successfully!")
    else:
        print("⚠️ Sample data creation failed or data already exists!")
    
    # Test queries
    print("\n4. Testing Database Queries...")
    try:
        # Test SELECT query
        books = execute_query("SELECT COUNT(*) as count FROM Books", fetch=True, fetch_all=False)
        print(f"✅ Found {books['count']} books in database")
        
        members = execute_query("SELECT COUNT(*) as count FROM Members", fetch=True, fetch_all=False)
        print(f"✅ Found {members['count']} members in database")
        
        borrowings = execute_query("SELECT COUNT(*) as count FROM Borrowing", fetch=True, fetch_all=False)
        print(f"✅ Found {borrowings['count']} borrowing records in database")
        
    except Exception as e:
        print(f"❌ Query test failed: {e}")
    
    # Close connection pool
    print("\n5. Closing Connection Pool...")
    db_manager.close_pool()
    print("✅ Connection pool closed successfully!")
    
    print("\n🎉 All tests completed successfully!")
    print("=" * 50)