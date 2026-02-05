"""
Manual Database Migration Script
Run this to add display_order column to existing database
"""
import sqlite3

def migrate_database():
    conn = sqlite3.connect('bot_platform.db')
    cursor = conn.cursor()
    
    print("=== DATABASE MIGRATION ===\n")
    
    # Check current columns
    print("Current columns in companies table:")
    cols = cursor.execute('PRAGMA table_info(companies)').fetchall()
    current_cols = [col[1] for col in cols]
    for col in cols:
        print(f"  - {col[1]} ({col[2]})")
    
    # Add display_order column if missing
    if 'display_order' not in current_cols:
        print("\n➕ Adding display_order column...")
        try:
            cursor.execute("ALTER TABLE companies ADD COLUMN display_order INTEGER DEFAULT 0")
            conn.commit()
            print("✅ display_order column added successfully!")
        except Exception as e:
            print(f"❌ Error adding display_order: {e}")
    else:
        print("\n✅ display_order column already exists")
    
    # Initialize display_order for existing companies
    print("\n🔄 Initializing display_order for existing companies...")
    companies = cursor.execute("SELECT id, bot_id FROM companies WHERE display_order IS NULL OR display_order = 0 ORDER BY id").fetchall()
    
    if companies:
        # Group by bot_id
        bots = {}
        for comp_id, bot_id in companies:
            if bot_id not in bots:
                bots[bot_id] = []
            bots[bot_id].append(comp_id)
        
        # Set sequential order for each bot
        for bot_id, comp_ids in bots.items():
            print(f"\n  Bot #{bot_id}: Ordering {len(comp_ids)} companies...")
            for idx, comp_id in enumerate(comp_ids):
                cursor.execute("UPDATE companies SET display_order = ? WHERE id = ?", (idx, comp_id))
            print(f"  ✅ Set order 0-{len(comp_ids)-1}")
        
        conn.commit()
        print(f"\n✅ Initialized display_order for {len(companies)} companies")
    else:
        print("  ℹ️ All companies already have display_order set")
    
    # Verify final state
    print("\n=== VERIFICATION ===")
    cols_after = cursor.execute('PRAGMA table_info(companies)').fetchall()
    print("\nFinal columns:")
    for col in cols_after:
        print(f"  - {col[1]} ({col[2]})")
    
    # Show sample data
    print("\nSample companies with display_order:")
    samples = cursor.execute("SELECT id, name, bot_id, display_order FROM companies LIMIT 10").fetchall()
    for s in samples:
        print(f"  ID {s[0]}: {s[1][:30]} (Bot #{s[2]}) - Order: {s[3]}")
    
    conn.close()
    print("\n✅ MIGRATION COMPLETE!")
    print("\nNext step: Restart the bot to load new code")

if __name__ == "__main__":
    migrate_database()
