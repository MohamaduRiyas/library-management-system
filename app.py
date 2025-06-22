import streamlit as st
import decimal
import mysql.connector
from datetime import datetime, date, timedelta
import pandas as pd
from db_connection import create_connection

# Page configuration
st.set_page_config(
    page_title="Library Management System",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(90deg, #f0f2f6, #ffffff);
        border-radius: 10px;
    }
    .metric-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

# Title with styling
st.markdown('<h1 class="main-header">📚 Library Management System</h1>', unsafe_allow_html=True)

# Sidebar menu
st.sidebar.markdown("## 🎯 Navigation Menu")
menu_options = [
    "📊 Dashboard",
    "📖 View Books", 
    "➕ Add Book",
    "👥 View Members", 
    "👤 Add Member",
    "📤 Borrow Book", 
    "📥 Return Book",
    "📋 Borrowing Records"
]

choice = st.sidebar.selectbox("Select Operation", menu_options)

# Helper functions
def get_connection():
    """Get database connection with error handling"""
    try:
        return create_connection()
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

def execute_query(query, params=None, fetch=False, fetch_all=True):
    """Execute database query with error handling"""
    connection = get_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        
        if fetch:
            result = cursor.fetchall() if fetch_all else cursor.fetchone()
        else:
            connection.commit()
            result = True
            
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return None

def get_dashboard_stats():
    """Get statistics for dashboard"""
    stats = {}
    
    # Total books
    result = execute_query("SELECT COUNT(*) as count FROM Books", fetch=True, fetch_all=False)
    stats['total_books'] = result['count'] if result else 0
    
    # Total members
    result = execute_query("SELECT COUNT(*) as count FROM Members", fetch=True, fetch_all=False)
    stats['total_members'] = result['count'] if result else 0
    
    # Currently borrowed books
    result = execute_query("SELECT COUNT(*) as count FROM Borrowing WHERE ReturnDate IS NULL", fetch=True, fetch_all=False)
    stats['borrowed_books'] = result['count'] if result else 0
    
    # Available copies
    result = execute_query("SELECT SUM(AvailableCopies) as count FROM Books", fetch=True, fetch_all=False)
    stats['available_copies'] = int(result['count']) if result and result['count'] is not None else 0
    
    return stats

# Dashboard
if choice == "📊 Dashboard":
    st.subheader("📊 Library Dashboard")
    
    # Get statistics
    stats = get_dashboard_stats()
    
    # Display metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📚 Total Books", stats['total_books'])
    with col2:
        st.metric("👥 Total Members", stats['total_members'])
    with col3:
        st.metric("📤 Books Borrowed", stats['borrowed_books'])
    with col4:
        st.metric("📖 Available Copies", stats['available_copies'])
    
    # Recent activity
    st.subheader("🕒 Recent Borrowing Activity")
    recent_activity = execute_query("""
        SELECT 
            b.BorrowID,
            m.Name as MemberName,
            bk.Title as BookTitle,
            b.BorrowDate,
            CASE WHEN b.ReturnDate IS NULL THEN 'Borrowed' ELSE 'Returned' END as Status
        FROM Borrowing b
        JOIN Members m ON b.MemberID = m.MemberID
        JOIN Books bk ON b.BookID = bk.BookID
        ORDER BY b.BorrowDate DESC
        LIMIT 10
    """, fetch=True)
    
    if recent_activity:
        df = pd.DataFrame(recent_activity)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No recent activity found.")

# View Books
elif choice == "📖 View Books":
    st.subheader("📖 Available Books")
    
    # Search functionality
    col1, col2 = st.columns([2, 1])
    with col1:
        search_term = st.text_input("🔍 Search books by title or author", "")
    with col2:
        min_copies = st.number_input("Min available copies", min_value=0, value=0)
    
    # Build query based on filters
    query = "SELECT BookID, Title, Author, PublishedYear, AvailableCopies FROM Books WHERE 1=1"
    params = []
    
    if search_term:
        query += " AND (Title LIKE %s OR Author LIKE %s)"
        params.extend([f"%{search_term}%", f"%{search_term}%"])
    
    if min_copies > 0:
        query += " AND AvailableCopies >= %s"
        params.append(min_copies)
    
    query += " ORDER BY Title"
    
    books = execute_query(query, params, fetch=True)
    
    if books:
        df = pd.DataFrame(books)
        st.dataframe(df, use_container_width=True)
        
        # Show availability summary
        available_books = len(df[df['AvailableCopies'] > 0])
        out_of_stock = len(df[df['AvailableCopies'] == 0])
        
        col1, col2 = st.columns(2)
        with col1:
            st.success(f"✅ {available_books} books available")
        with col2:
            st.error(f"❌ {out_of_stock} books out of stock")
    else:
        st.warning("No books found matching your criteria.")

# Add Book
elif choice == "➕ Add Book":
    st.subheader("➕ Add New Book")
    
    with st.form("add_book_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            title = st.text_input("📖 Book Title*", help="Enter the complete title of the book")
            author = st.text_input("✍️ Author*", help="Enter the author's name")
        
        with col2:
            year = st.number_input("📅 Published Year*", min_value=1900, max_value=2030, value=2023)
            copies = st.number_input("📚 Available Copies*", min_value=1, value=1)
        
        submit = st.form_submit_button("➕ Add Book", use_container_width=True)
        
        if submit:
            if title and author:
                # Check if book already exists
                existing_book = execute_query(
                    "SELECT BookID FROM Books WHERE Title = %s AND Author = %s",
                    (title, author), fetch=True, fetch_all=False
                )
                
                if existing_book:
                    st.warning("⚠️ A book with this title and author already exists!")
                else:
                    # Use only the columns that exist in your schema
                    query = "INSERT INTO Books (Title, Author, PublishedYear, AvailableCopies) VALUES (%s, %s, %s, %s)"
                    params = (title, author, year, copies)
                    
                    if execute_query(query, params):
                        st.success("✅ Book added successfully!")
                        st.balloons()
                    else:
                        st.error("❌ Failed to add book. Please try again.")
            else:
                st.error("❌ Please fill in all required fields (marked with *).")

# View Members
elif choice == "👥 View Members":
    st.subheader("👥 Registered Members")
    
    # Search functionality
    search_member = st.text_input("🔍 Search members by name or email", "")
    
    query = "SELECT MemberID, Name, Email, PhoneNumber FROM Members WHERE 1=1"
    params = []
    
    if search_member:
        query += " AND (Name LIKE %s OR Email LIKE %s)"
        params.extend([f"%{search_member}%", f"%{search_member}%"])
    
    query += " ORDER BY Name"
    
    members = execute_query(query, params, fetch=True)
    
    if members:
        df = pd.DataFrame(members)
        st.dataframe(df, use_container_width=True)
        
        # Show member statistics
        total_members = len(df)
        st.metric("👥 Total Members", total_members)
    else:
        st.warning("No members found.")

# Add Member
elif choice == "👤 Add Member":
    st.subheader("👤 Add New Member")
    
    with st.form("add_member_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("👤 Member Name*", help="Full name of the member")
            email = st.text_input("📧 Email*", help="Valid email address")
        
        with col2:
            phone = st.text_input("📱 Phone Number", help="Contact number (optional)")
        
        submit = st.form_submit_button("👤 Add Member", use_container_width=True)
        
        if submit:
            if name and email:
                # Validate email format (basic check)
                if "@" not in email or "." not in email:
                    st.error("❌ Please enter a valid email address.")
                else:
                    # Use only the columns that exist in your schema
                    query = "INSERT INTO Members (Name, Email, PhoneNumber) VALUES (%s, %s, %s)"
                    params = (name, email, phone if phone else None)
                    
                    result = execute_query(query, params)
                    if result:
                        st.success("✅ Member added successfully!")
                        st.balloons()
                    else:
                        st.error("❌ Failed to add member. Email might already exist.")
            else:
                st.error("❌ Please fill in all required fields (marked with *).")

# Borrow Book
elif choice == "📤 Borrow Book":
    st.subheader("📤 Borrow a Book")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Member selection
        members = execute_query("SELECT MemberID, Name FROM Members ORDER BY Name", fetch=True)
        if members:
            member_options = {f"{m['Name']} (ID: {m['MemberID']})": m['MemberID'] for m in members}
            selected_member = st.selectbox("👤 Select Member", list(member_options.keys()))
            member_id = member_options[selected_member] if selected_member else None
        else:
            st.error("No members found. Please add members first.")
            member_id = None
    
    with col2:
        # Book selection (only available books)
        books = execute_query(
            "SELECT BookID, Title, Author, AvailableCopies FROM Books WHERE AvailableCopies > 0 ORDER BY Title", 
            fetch=True
        )
        if books:
            book_options = {f"{b['Title']} by {b['Author']} (Available: {b['AvailableCopies']})": b['BookID'] for b in books}
            selected_book = st.selectbox("📖 Select Book", list(book_options.keys()))
            book_id = book_options[selected_book] if selected_book else None
        else:
            st.error("No available books found.")
            book_id = None
    
    if st.button("📤 Borrow Book", use_container_width=True):
        if member_id and book_id:
            # Check if member already has this book
            existing_borrow = execute_query(
                "SELECT BorrowID FROM Borrowing WHERE MemberID = %s AND BookID = %s AND ReturnDate IS NULL",
                (member_id, book_id), fetch=True, fetch_all=False
            )
            
            if existing_borrow:
                st.error("❌ This member has already borrowed this book and hasn't returned it yet.")
            else:
                # Process borrowing - using only existing columns
                borrow_query = "INSERT INTO Borrowing (MemberID, BookID, BorrowDate) VALUES (%s, %s, CURRENT_DATE)"
                update_query = "UPDATE Books SET AvailableCopies = AvailableCopies - 1 WHERE BookID = %s"
                
                if execute_query(borrow_query, (member_id, book_id)):
                    if execute_query(update_query, (book_id,)):
                        st.success("✅ Book borrowed successfully!")
                        st.balloons()
                    else:
                        st.error("❌ Failed to update book availability.")
                else:
                    st.error("❌ Failed to process borrowing.")
        else:
            st.error("❌ Please select both member and book.")

# Return Book
elif choice == "📥 Return Book":
    st.subheader("📥 Return a Book")
    
    # Show borrowed books
    borrowed_books = execute_query("""
        SELECT 
            b.BorrowID,
            m.Name as MemberName,
            bk.Title as BookTitle,
            b.BorrowDate,
            DATEDIFF(CURRENT_DATE, b.BorrowDate) as DaysBorrowed
        FROM Borrowing b
        JOIN Members m ON b.MemberID = m.MemberID
        JOIN Books bk ON b.BookID = bk.BookID
        WHERE b.ReturnDate IS NULL
        ORDER BY b.BorrowDate
    """, fetch=True)
    
    if borrowed_books:
        # Create a dropdown for borrowed books
        book_options = {}
        for book in borrowed_books:
            days_text = f" ({book['DaysBorrowed']} days ago)" if book['DaysBorrowed'] > 0 else " (Today)"
            option_text = f"{book['BookTitle']} - {book['MemberName']}{days_text}"
            book_options[option_text] = book['BorrowID']
        
        selected_return = st.selectbox("📖 Select book to return", list(book_options.keys()))
        borrow_id = book_options[selected_return] if selected_return else None
        
        # Show details of selected borrowing
        if borrow_id:
            selected_book_info = next(b for b in borrowed_books if b['BorrowID'] == borrow_id)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📖 **Book:** {selected_book_info['BookTitle']}")
            with col2:
                st.info(f"👤 **Member:** {selected_book_info['MemberName']}")
            with col3:
                st.info(f"📅 **Borrowed:** {selected_book_info['DaysBorrowed']} days ago")
        
        if st.button("📥 Return Book", use_container_width=True):
            if borrow_id:
                # Update borrowing record
                return_query = "UPDATE Borrowing SET ReturnDate = CURRENT_DATE WHERE BorrowID = %s"
                
                # Get book ID to update availability
                book_info = execute_query(
                    "SELECT BookID FROM Borrowing WHERE BorrowID = %s",
                    (borrow_id,), fetch=True, fetch_all=False
                )
                
                if book_info and execute_query(return_query, (borrow_id,)):
                    # Update book availability
                    update_query = "UPDATE Books SET AvailableCopies = AvailableCopies + 1 WHERE BookID = %s"
                    if execute_query(update_query, (book_info['BookID'],)):
                        st.success("✅ Book returned successfully!")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ Failed to update book availability.")
                else:
                    st.error("❌ Failed to process return.")
    else:
        st.info("📚 No books are currently borrowed.")

# Borrowing Records
elif choice == "📋 Borrowing Records":
    st.subheader("📋 Borrowing Records")
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.selectbox("Status", ["All", "Borrowed", "Returned"])
    with col2:
        member_search = st.text_input("Member name")
    
    # Build query
    query = """
        SELECT 
            b.BorrowID,
            m.Name as MemberName,
            bk.Title as BookTitle,
            b.BorrowDate,
            b.ReturnDate,
            CASE 
                WHEN b.ReturnDate IS NULL THEN 'Borrowed'
                ELSE 'Returned'
            END as Status
        FROM Borrowing b
        JOIN Members m ON b.MemberID = m.MemberID
        JOIN Books bk ON b.BookID = bk.BookID
        WHERE 1=1
    """
    params = []
    
    if status_filter != "All":
        if status_filter == "Borrowed":
            query += " AND b.ReturnDate IS NULL"
        elif status_filter == "Returned":
            query += " AND b.ReturnDate IS NOT NULL"
    
    if member_search:
        query += " AND m.Name LIKE %s"
        params.append(f"%{member_search}%")
    
    query += " ORDER BY b.BorrowDate DESC"
    
    records = execute_query(query, params, fetch=True)
    
    if records:
        df = pd.DataFrame(records)
        st.dataframe(df, use_container_width=True)
        
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Records", len(df))
        with col2:
            borrowed_count = len(df[df['Status'] == 'Borrowed'])
            st.metric("📤 Currently Borrowed", borrowed_count)
        with col3:
            returned_count = len(df[df['Status'] == 'Returned'])
            st.metric("📥 Returned", returned_count)
    else:
        st.info("No borrowing records found.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p>📚 Library Management System | Built with Streamlit</p>
    <p>💡 Compatible with your existing database schema</p>
</div>
""", unsafe_allow_html=True)