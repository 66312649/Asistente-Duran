/**
 * N8N Data Filter Function
 * Extracts only message and conversation_id from incoming request
 */

function filterRequestData(incomingData) {
  // Initialize result object
  const result = {
    message: null,
    conversation_id: null
  };

  // Extract message field (check multiple possible field names)
  if (incomingData.message) {
    result.message = incomingData.message;
  } else if (incomingData.text) {
    result.message = incomingData.text;
  } else if (incomingData.content) {
    result.message = incomingData.content;
  } else if (incomingData.body && incomingData.body.message) {
    result.message = incomingData.body.message;
  }

  // Extract conversation ID field (check multiple possible field names)
  if (incomingData.conversation_id) {
    result.conversation_id = incomingData.conversation_id;
  } else if (incomingData.chat_id) {
    result.conversation_id = incomingData.chat_id;
  } else if (incomingData.session_id) {
    result.conversation_id = incomingData.session_id;
  } else if (incomingData.timestamp) {
    // Use timestamp as fallback conversation identifier
    result.conversation_id = incomingData.timestamp;
  } else if (incomingData.center) {
    // Use center as conversation context identifier
    result.conversation_id = `${incomingData.center}_${Date.now()}`;
  }

  return result;
}

// Example usage based on the webhook data structure shown:
// Input from your webhook would be something like:
const exampleInput = {
  message: "hola",
  language: "es", 
  center: "calvia",
  centerLabel: "Duran Calvià",
  timestamp: "2025-09-11T13:40:40.657Z",
  articles: [...],
  locations: [...],
  articlesCount: 18290,
  locationsCount: 5
};

// Filtered output:
const filteredData = filterRequestData(exampleInput);
console.log(JSON.stringify(filteredData, null, 2));

// Expected output:
// {
//   "message": "hola",
//   "conversation_id": "2025-09-11T13:40:40.657Z"
// }

// Export for N8N usage
module.exports = { filterRequestData };