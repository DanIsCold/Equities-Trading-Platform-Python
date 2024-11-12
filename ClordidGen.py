import random
import os
#generates clordids for orders(should only be called when creating new order)
#call clear_order_id to clear the txt file
class clordidGenerator:
    def __init__(self, file_name='clordid_list.txt'):
        self.file_name = file_name
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'w') as file:
                file.write('')
        self.existing_ids = self.load_existing_ids()

    def generate_order_id(self):
        while True:
            # Generate a unique order ID with 'DE' followed by 14 random digits
            order_id = 'DE' + ''.join(random.choices('0123456789', k=14))
            # Check if the ID is unique
            if order_id not in self.existing_ids:
                # Add the new ID to the set and save it to the file
                self.existing_ids.add(order_id)
                self.save_to_file(order_id)
                return order_id

    def load_existing_ids(self):
        # Load all IDs from the file into a set for quick lookups
        if os.path.getsize(self.file_name) == 0:
            return set()
        
        with open(self.file_name, 'r') as file:
            data = file.read()
            if data:
                return set(data.split(','))
            else:
                return set()

    def save_to_file(self, order_id):
        # Append the new order ID to the file with a comma separator
        with open(self.file_name, 'a') as file:
            # Check if the file is empty before writing a comma
            if os.path.getsize(self.file_name) > 0:
                file.write(',')
            file.write(order_id)

    def clear_order_id(self):
        # Clear the contents of the file and reset the existing IDs set
        with open(self.file_name, 'w') as file:
            file.write('')
        self.existing_ids.clear()
        print(f"The file '{self.file_name}' has been cleared.")

# Example usage:
# generator = clordidGenerator()
# new_id = generator.generate_order_id()
# print("Generated Order ID:", new_id)
