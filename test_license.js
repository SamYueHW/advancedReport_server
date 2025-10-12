const licenseService = require('./services/licenseService');

async function testLicenseService() {
    console.log('🧪 Testing License Service...\n');
    
    try {
        // 1. Initialize the service
        console.log('1. Initializing license service...');
        const initialized = await licenseService.initialize();
        console.log(`   Result: ${initialized ? '✅ Success' : '❌ Failed'}\n`);
        
        if (!initialized) {
            console.log('❌ Cannot continue testing without database connection');
            return;
        }
        
        // 2. Health check
        console.log('2. Health check...');
        const health = await licenseService.healthCheck();
        console.log(`   Status: ${health.status}`);
        console.log(`   Database: ${health.database}`);
        if (health.error) {
            console.log(`   Error: ${health.error}`);
        }
        console.log();
        
        // 3. Get all Advanced Report stores
        console.log('3. Getting all Advanced Report stores...');
        const stores = await licenseService.getAllAdvancedReportStores();
        console.log(`   Found ${stores.length} stores:`);
        stores.forEach((store, index) => {
            console.log(`   ${index + 1}. Store ${store.storeId} (${store.storeName})`);
            console.log(`      App ID: ${store.appId}`);
            console.log(`      Expires: ${store.expireDate}`);
            console.log(`      Status: ${store.isExpired ? '❌ Expired' : '✅ Valid'}`);
        });
        console.log();
        
        // 4. Test license validation for a specific store
        if (stores.length > 0) {
            const testStore = stores[0];
            console.log(`4. Testing license validation for Store ${testStore.storeId}...`);
            
            const validation = await licenseService.validateAdvancedReportLicense(
                testStore.storeId, 
                testStore.appId
            );
            
            console.log(`   Store ID: ${testStore.storeId}`);
            console.log(`   App ID: ${testStore.appId}`);
            console.log(`   Is Valid: ${validation.isValid ? '✅ Yes' : '❌ No'}`);
            console.log(`   Is Expired: ${validation.isExpired ? '❌ Yes' : '✅ No'}`);
            
            if (validation.storeInfo) {
                const info = validation.storeInfo;
                console.log(`   Store Name: ${info.storeName}`);
                console.log(`   Expire Date: ${info.expireDate}`);
                console.log(`   Days Remaining: ${info.daysRemaining}`);
            }
            
            if (validation.error) {
                console.log(`   Error: ${validation.error}`);
            }
        } else {
            console.log('4. No stores found for testing license validation');
        }
        
        // 5. Test with invalid store (should fail)
        console.log('\n5. Testing with invalid store ID...');
        const invalidValidation = await licenseService.validateAdvancedReportLicense(
            'INVALID_STORE', 
            'INVALID_APP_ID'
        );
        console.log(`   Is Valid: ${invalidValidation.isValid ? '✅ Yes' : '❌ No'} (Expected: No)`);
        console.log(`   Error: ${invalidValidation.error}`);
        
    } catch (error) {
        console.error('❌ Test failed:', error.message);
    } finally {
        // Close the connection
        await licenseService.close();
        console.log('\n🔒 License service connection closed');
        console.log('✅ Test completed');
    }
}

// Handle environment setup
require('dotenv').config();

// Run the test
testLicenseService().catch(console.error);
