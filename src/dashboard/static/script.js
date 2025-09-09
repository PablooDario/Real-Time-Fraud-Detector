const countryCoords = {
            'US': [39.8283, -98.5795], 'CA': [56.1304, -106.3468], 'MX': [23.6345, -102.5528],
            'GB': [55.3781, -3.4360], 'FR': [46.2276, 2.2137], 'DE': [51.1657, 10.4515],
            'IT': [41.8719, 12.5674], 'ES': [40.4637, -3.7492], 'RU': [61.5240, 105.3188],
            'CN': [35.8617, 104.1954], 'JP': [36.2048, 138.2529], 'KR': [35.9078, 127.7669],
            'IN': [20.5937, 78.9629], 'BR': [-14.2350, -51.9253], 'AR': [-38.4161, -63.6167],
            'AU': [-25.2744, 133.7751], 'ZA': [-30.5595, 22.9375], 'EG': [26.0975, 30.0444],
            'NG': [9.0820, 8.6753], 'KE': [-0.0236, 37.9062], 'TR': [38.9637, 35.2433],
            'SA': [23.8859, 45.0792], 'AE': [23.4241, 53.8478], 'IL': [31.0461, 34.8516],
            'TH': [15.8700, 100.9925], 'VN': [14.0583, 108.2772], 'SG': [1.3521, 103.8198],
            'MY': [4.2105, 101.9758], 'ID': [-0.7893, 113.9213], 'PH': [12.8797, 121.7740],
            'PK': [30.3753, 69.3451], 'BD': [23.6850, 90.3563], 'LK': [7.8731, 80.7718],
            'AF': [33.9391, 67.7100], 'IQ': [33.2232, 43.6793], 'IR': [32.4279, 53.6880],
            'PS': [31.9522, 35.2332], 'LB': [33.8547, 35.8623], 'JO': [30.5852, 36.2384],
            'CL': [-35.6751, -71.5430], 'PE': [-9.1900, -75.0152], 'CO': [4.5709, -74.2973],
            'VE': [6.4238, -66.5897], 'EC': [-1.8312, -78.1834], 'BO': [-16.2902, -63.5887],
            'PY': [-23.4425, -58.4438], 'UY': [-32.5228, -55.7658], 'GY': [4.8604, -58.9302],
            'SR': [3.9193, -56.0278], 'GF': [3.9339, -53.1258], 'FK': [-51.7963, -59.5236],
            'CF': [6.6111, 20.9394], 'TD': [15.4542, 18.7322], 'CM': [7.3697, 12.3547],
            'GA': [-0.8037, 11.6094], 'GQ': [1.6508, 10.2679], 'CG': [-0.2280, 15.8277],
            'CD': [-4.0383, 21.7587], 'AO': [-11.2027, 17.8739], 'ZM': [-13.1339, 27.8493],
            'ZW': [-19.0154, 29.1549], 'BW': [-22.3285, 24.6849], 'NA': [-22.9576, 18.4904],
            'SZ': [-26.5225, 31.4659], 'LS': [-29.6099, 28.2336], 'MW': [-13.2543, 34.3015],
            'MZ': [-18.6657, 35.5296], 'MG': [-18.7669, 46.8691], 'MU': [-20.3484, 57.5522],
            'SC': [-4.6796, 55.4920], 'KM': [-11.8750, 43.8722], 'YT': [-12.8275, 45.1662],
            'RE': [-21.1151, 55.5364], 'SH': [-24.1434, -10.0307], 'IO': [-6.3432, 71.8765],
        };

// Initialize map
const map = L.map('map').setView([20, 0], 2);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors'
}).addTo(map);

// Data storage
let transactions = [];
let frauds = [];
let totalCount = 0;
let fraudCount = 0;

// Update stats
function updateStats() {
    document.getElementById('totalCount').textContent = totalCount;
    document.getElementById('fraudCount').textContent = fraudCount;
    const rate = totalCount > 0 ? ((fraudCount / totalCount) * 100).toFixed(1) : 0;
    document.getElementById('fraudRate').textContent = rate + '%';
}

// Add transaction marker to map
function addTransactionMarker(location) {
    const coords = countryCoords[location];
    if (!coords) return;

    const marker = L.circleMarker(coords, {
        color: '#ff0000',
        fillColor: '#ff0000',
        fillOpacity: 0.8,
        radius: 8
    }).addTo(map);

    // Animate marker disappearance
    let opacity = 0.8;
    const fadeInterval = setInterval(() => {
        opacity -= 0.05;
        marker.setStyle({ fillOpacity: opacity, opacity: opacity });
        
        if (opacity <= 0) {
            map.removeLayer(marker);
            clearInterval(fadeInterval);
        }
    }, 150); // 3 seconds total (0.05 * 60 steps * 150ms)
}

// Update transactions table
function updateTransactionsTable() {
    const tbody = document.getElementById('transactionsList');
    tbody.innerHTML = '';
    
    transactions.slice(0, 10).forEach((tx, index) => {
        const row = document.createElement('tr');
        if (index === 0) row.classList.add('new-transaction');
        if (tx.is_fraud_predicted === 1) row.classList.add('fraud-row');
        
        row.innerHTML = `
            <td>${tx.user_id}</td>
            <td class="amount">$${tx.amount.toFixed(2)}</td>
            <td>${tx.category}</td>
            <td class="location">${tx.location}</td>
            <td>${tx.is_fraud_predicted === 1 ? '🚨 YES' : '✅ NO'}</td>
        `;
        tbody.appendChild(row);
    });
}

// Update frauds table
function updateFraudsTable() {
    const tbody = document.getElementById('fraudsList');
    tbody.innerHTML = '';
    
    frauds.slice(0, 10).forEach((tx, index) => {
        const row = document.createElement('tr');
        if (index === 0) row.classList.add('new-transaction');
        
        row.innerHTML = `
            <td>${tx.user_id}</td>
            <td class="amount">$${tx.amount.toFixed(2)}</td>
            <td>${tx.category}</td>
            <td class="location">${tx.location}</td>
            <td>${(tx.fraud_probability * 100).toFixed(1)}%</td>
        `;
        tbody.appendChild(row);
    });
}

// Process new transactions
function processTransactions(newTransactions) {
    newTransactions.forEach(tx => {
        totalCount++;
        
        // Add to front of transactions array
        transactions.unshift(tx);
        if (transactions.length > 50) transactions.pop();
        
        // Add to frauds if predicted as fraud
        if (tx.is_fraud_predicted === 1) {
            fraudCount++;
            frauds.unshift(tx);
            if (frauds.length > 50) frauds.pop();
        }
        
        // Add marker to map
        addTransactionMarker(tx.location);
    });
    
    updateStats();
    updateTransactionsTable();
    updateFraudsTable();
}

// Fetch transactions from API
async function fetchTransactions() {
    try {
        const response = await fetch('/transactions');
        const data = await response.json();
        
        if (data.length > 0) {
            processTransactions(data);
        }
    } catch (error) {
        console.error('Error fetching transactions:', error);
    }
}

// Start polling for new transactions
setInterval(fetchTransactions, 2000); // Check every 2 seconds

// Initial load
fetchTransactions();
